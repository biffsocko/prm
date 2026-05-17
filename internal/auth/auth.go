// Package auth handles password hashing and the SASL-style three-frame
// auth handshake.
//
// Hashing: Argon2id with the parameters documented in DESIGN.md
// (m=64 MiB, t=3, p=1, 16-byte salt, 32-byte output). These are baked into
// the per-account `password_params` string and recomputed on verify so the
// parameters can be tuned later without invalidating existing passwords.
//
// Handshake state machine (server side):
//
//	state                 incoming frame        outgoing frame        next state
//	---------------------------------------------------------------------------
//	awaitingRequest    -> AuthRequest        -> AuthChallenge      -> awaitingResponse
//	awaitingResponse   -> AuthResponse       -> AuthOK / AuthErr   -> done
//
// Token-method auth (slice 2+) collapses to a one-shot
// AuthRequest{Method:"token",Token:...} -> AuthOK / AuthErr.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/crypto/argon2"

	"github.com/biffsocko/prm/internal/storage"
)

// Argon2id parameters. Tuned per DESIGN.md.
const (
	argonMemory      = 64 * 1024 // KiB (= 64 MiB)
	argonIterations  = 3
	argonParallelism = 1
	argonSaltLen     = 16
	argonKeyLen      = 32
)

// ParamsString is the canonical parameter string stored alongside every
// password hash. Future tuning changes the constant; old hashes continue
// to verify because their own params are stored per-account.
const ParamsString = "argon2id,m=65536,t=3,p=1,k=32"

// Sentinel errors. Reasons surface in AuthErr.Reason on the wire.
var (
	ErrUnauthenticated = errors.New("auth: invalid credentials")
	ErrUnsupported     = errors.New("auth: unsupported method")
	ErrTenantNotFound  = errors.New("auth: tenant not found")
	ErrTenantSuspended = errors.New("auth: tenant suspended")
)

// HashPassword returns a fresh salt + Argon2id hash of the password using
// the current ParamsString. Use this when creating an account.
func HashPassword(password string) (hash, salt []byte, params string, err error) {
	salt = make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, nil, "", fmt.Errorf("auth: generate salt: %w", err)
	}
	hash = argon2.IDKey([]byte(password), salt, argonIterations, argonMemory, argonParallelism, argonKeyLen)
	return hash, salt, ParamsString, nil
}

// VerifyPassword constant-time-compares an Argon2id hash of the candidate
// password (using the account's stored salt + params) against the stored
// hash. Returns nil on match, ErrUnauthenticated on mismatch.
func VerifyPassword(stored *storage.Account, candidate string) error {
	if stored == nil {
		return ErrUnauthenticated
	}
	mem, iters, par, keyLen, err := parseParams(stored.PasswordParams)
	if err != nil {
		// Stored params are corrupt; treat as auth failure (don't leak detail).
		return ErrUnauthenticated
	}
	candidateHash := argon2.IDKey([]byte(candidate), stored.PasswordSalt, iters, mem, par, keyLen)
	if subtle.ConstantTimeCompare(candidateHash, stored.PasswordHash) != 1 {
		return ErrUnauthenticated
	}
	return nil
}

// parseParams extracts m, t, p, k from a stored params string like
// "argon2id,m=65536,t=3,p=1,k=32". Forgiving: missing k defaults to
// argonKeyLen. Returns ErrUnauthenticated-style failures up the stack
// when the params are unparseable.
func parseParams(s string) (mem uint32, iters uint32, par uint8, keyLen uint32, err error) {
	keyLen = argonKeyLen
	if !strings.HasPrefix(s, "argon2id,") {
		return 0, 0, 0, 0, fmt.Errorf("not argon2id params: %q", s)
	}
	for _, kv := range strings.Split(strings.TrimPrefix(s, "argon2id,"), ",") {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k, v := kv[:eq], kv[eq+1:]
		n, perr := strconv.ParseUint(v, 10, 32)
		if perr != nil {
			return 0, 0, 0, 0, fmt.Errorf("bad numeric for %s: %w", k, perr)
		}
		switch k {
		case "m":
			mem = uint32(n)
		case "t":
			iters = uint32(n)
		case "p":
			par = uint8(n)
		case "k":
			keyLen = uint32(n)
		}
	}
	if mem == 0 || iters == 0 || par == 0 {
		return 0, 0, 0, 0, fmt.Errorf("missing required params in %q", s)
	}
	return mem, iters, par, keyLen, nil
}

// Result is returned by the handshake when auth completes. AccountID is
// non-zero on success; OK indicates whether the handshake completed
// successfully.
type Result struct {
	OK       bool
	Account  *storage.Account
	Tenant   *storage.Tenant
	Reason   string // machine-readable code for AuthErr; populated when !OK
	Detail   string // optional human text (don't leak sensitive info)
}

// Challenge is what the server returns from BeginPasswordAuth — pass the
// Nonce and Salt to the client in AuthChallenge, then call CompletePasswordAuth
// when the AuthResponse arrives.
type Challenge struct {
	AccountID uuid.UUID
	TenantID  uuid.UUID
	Nonce     []byte
	Salt      []byte // the account's salt (cleartext-safe; salts are not secret)
	Params    string
}

// BeginPasswordAuth starts the password-method handshake. Looks up the
// account, generates a server nonce, returns the data the server should
// put into the AuthChallenge frame. If the account does not exist or the
// tenant is suspended, returns an error and the server should reply with
// AuthErr (without leaking which condition failed).
//
// NOTE: For now the "proof" computation does NOT use the nonce — the
// client sends an Argon2id hash of the password and salt, and we
// constant-time-compare to the stored hash. Adding nonce-based challenge
// (a la SCRAM) is a slice 5+ hardening; the wire shape already has the
// nonce field so we can upgrade without breaking the protocol.
func BeginPasswordAuth(ctx context.Context, s storage.Store, tenantSlug, username string) (*Challenge, *storage.Tenant, error) {
	tenant, err := s.GetTenantBySlug(ctx, tenantSlug)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil, ErrTenantNotFound
		}
		return nil, nil, fmt.Errorf("auth begin: lookup tenant: %w", err)
	}
	if tenant.Status == storage.TenantSuspended {
		return nil, nil, ErrTenantSuspended
	}
	acc, err := s.GetAccountByUsername(ctx, tenant.ID, username)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, tenant, ErrUnauthenticated
		}
		return nil, tenant, fmt.Errorf("auth begin: lookup account: %w", err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, tenant, fmt.Errorf("auth begin: nonce: %w", err)
	}
	return &Challenge{
		AccountID: acc.ID,
		TenantID:  tenant.ID,
		Nonce:     nonce,
		Salt:      acc.PasswordSalt,
		Params:    acc.PasswordParams,
	}, tenant, nil
}

// CompletePasswordAuth verifies the client's proof against the stored hash.
// proofBase64 is what the client sent in AuthResponse.Proof — the base64
// of the Argon2id hash output computed with the account's salt + params.
//
// Returns a Result; the caller turns it into AuthOK or AuthErr on the wire.
func CompletePasswordAuth(ctx context.Context, s storage.Store, ch *Challenge, proofBase64 string) (*Result, error) {
	proof, err := base64.StdEncoding.DecodeString(proofBase64)
	if err != nil {
		return &Result{Reason: "invalid_credentials"}, nil
	}
	acc, err := s.GetAccountByID(ctx, ch.TenantID, ch.AccountID)
	if err != nil {
		return &Result{Reason: "invalid_credentials"}, nil
	}
	if subtle.ConstantTimeCompare(proof, acc.PasswordHash) != 1 {
		return &Result{Reason: "invalid_credentials"}, nil
	}
	tenant, err := s.GetTenantByID(ctx, acc.TenantID)
	if err != nil {
		return nil, fmt.Errorf("auth complete: lookup tenant: %w", err)
	}
	return &Result{OK: true, Account: acc, Tenant: tenant}, nil
}

// EncodeBase64 is a convenience for clients computing the proof bytes.
func EncodeBase64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// DecodeBase64 is the inverse for clients reading the salt/nonce from the
// challenge frame.
func DecodeBase64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

// ComputeClientProof is what a client calls to produce the proof bytes for
// AuthResponse.Proof. Equivalent to HashPassword's hashing step, using the
// salt + params from the AuthChallenge frame.
func ComputeClientProof(password string, salt []byte, params string) ([]byte, error) {
	mem, iters, par, keyLen, err := parseParams(params)
	if err != nil {
		return nil, fmt.Errorf("auth client proof: %w", err)
	}
	return argon2.IDKey([]byte(password), salt, iters, mem, par, keyLen), nil
}

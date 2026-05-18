package proto

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize is the maximum allowed length of one JSON line, in bytes.
// Frames larger than this cause the decoder to return ErrFrameTooLarge and
// close the underlying connection (per spec; oversized frames are not
// recoverable inside a stream).
const MaxFrameSize = 64 * 1024

// ErrFrameTooLarge is returned by the Decoder when an incoming frame exceeds
// MaxFrameSize.
var ErrFrameTooLarge = errors.New("proto: frame exceeds 64 KB")

// ErrUnknownType is returned by Decode when the "type" field is not a verb
// recognized by this codec build.
var ErrUnknownType = errors.New("proto: unknown frame type")

// Encode writes a single frame to w as a JSON object followed by '\n'.
// It guarantees one Write per call when w is a bufio.Writer.
//
// The frame's Type field is forced to match its FrameType() — callers can
// leave Type empty when constructing frames; Encode fills it in.
func Encode(w io.Writer, f Frame) error {
	// Force the Type field via the same trick everywhere: marshal, then read
	// back into a generic map so we can stamp "type" unconditionally. For the
	// hot path, callers should precompute frames with Type already set so we
	// can skip this.
	stamped, err := marshalWithType(f)
	if err != nil {
		return err
	}
	if len(stamped) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	stamped = append(stamped, '\n')
	_, err = w.Write(stamped)
	return err
}

// EncodeBytes returns the wire bytes for a frame (including the trailing
// newline) without writing them anywhere. This is the hot-path helper used
// by the broadcast fan-out — compute the wire bytes once, push the same
// []byte onto every member's outbound queue.
func EncodeBytes(f Frame) ([]byte, error) {
	stamped, err := marshalWithType(f)
	if err != nil {
		return nil, err
	}
	if len(stamped) > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	return append(stamped, '\n'), nil
}

func marshalWithType(f Frame) ([]byte, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("proto: marshal: %w", err)
	}
	// Quick check: if the JSON already contains "type":"<verb>" we're done.
	// Otherwise fall through to a re-marshal that injects it. This is a
	// belt-and-suspenders guard — every Frame struct embeds Type as a
	// json:"type" field, so the normal path skips the re-marshal entirely.
	wantType := f.FrameType()
	// Inspect the first ~64 bytes for the type field. JSON marshal of a
	// struct with `json:"type"` as a non-empty string will start with
	// {"type":"... or contain it early. If the Type field was empty when
	// the struct was passed in, the verb name won't appear; we patch it.
	if !containsType(raw, wantType) {
		var asMap map[string]any
		if err := json.Unmarshal(raw, &asMap); err != nil {
			return nil, fmt.Errorf("proto: remarshal: %w", err)
		}
		asMap["type"] = wantType
		raw, err = json.Marshal(asMap)
		if err != nil {
			return nil, fmt.Errorf("proto: remarshal: %w", err)
		}
	}
	return raw, nil
}

// containsType returns true if the JSON object's "type" field equals want.
// Cheap substring check — the field is at the top of the object in practice.
func containsType(raw []byte, want string) bool {
	needle := []byte(`"type":"` + want + `"`)
	return bytesContains(raw, needle)
}

func bytesContains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
outer:
	for i := 0; i <= len(haystack)-len(needle); i++ {
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return true
	}
	return false
}

// Decoder reads JSON-line frames from an underlying io.Reader.
//
// Decoder uses a bufio.Scanner with the MaxFrameSize buffer. It is not safe
// for concurrent use; one Decoder per connection.
type Decoder struct {
	s *bufio.Scanner
}

// NewDecoder wraps r in a Decoder. Pre-sizes the buffer to MaxFrameSize so
// no allocation happens per Decode call.
func NewDecoder(r io.Reader) *Decoder {
	s := bufio.NewScanner(r)
	buf := make([]byte, 0, 4096)
	s.Buffer(buf, MaxFrameSize)
	return &Decoder{s: s}
}

// envelope is the minimal shape we parse to learn the frame's type.
type envelope struct {
	Type string `json:"type"`
}

// Decode reads the next frame off the wire and returns it as a Frame.
// Returns io.EOF when the stream closes cleanly.
func (d *Decoder) Decode() (Frame, error) {
	if !d.s.Scan() {
		if err := d.s.Err(); err != nil {
			if errors.Is(err, bufio.ErrTooLong) {
				return nil, ErrFrameTooLarge
			}
			return nil, err
		}
		return nil, io.EOF
	}
	line := d.s.Bytes()
	if len(line) == 0 {
		return d.Decode() // skip blank lines
	}

	var env envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return nil, fmt.Errorf("proto: decode envelope: %w", err)
	}

	frame, err := frameForType(env.Type)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(line, frame); err != nil {
		return nil, fmt.Errorf("proto: decode %s: %w", env.Type, err)
	}
	// frame is a *Hello, *Welcome, etc. Dereference so callers get a value.
	return derefFrame(frame), nil
}

// frameForType returns a pointer to a zero-value frame struct matching the
// given verb type, or ErrUnknownType.
func frameForType(t string) (Frame, error) {
	switch t {
	case TypeHello:
		return &Hello{}, nil
	case TypeWelcome:
		return &Welcome{}, nil
	case TypeAuthRequest:
		return &AuthRequest{}, nil
	case TypeAuthChallenge:
		return &AuthChallenge{}, nil
	case TypeAuthResponse:
		return &AuthResponse{}, nil
	case TypeAuthOK:
		return &AuthOK{}, nil
	case TypeAuthErr:
		return &AuthErr{}, nil
	case TypeJoin:
		return &Join{}, nil
	case TypePart:
		return &Part{}, nil
	case TypeMsg:
		return &Msg{}, nil
	case TypePresence:
		return &Presence{}, nil
	case TypePing:
		return &Ping{}, nil
	case TypePong:
		return &Pong{}, nil
	case TypeError:
		return &Error{}, nil
	case TypeSubscriptionCreate:
		return &SubscriptionCreate{}, nil
	case TypeSubscriptionList:
		return &SubscriptionList{}, nil
	case TypeSubscriptionGet:
		return &SubscriptionGet{}, nil
	case TypeSubscriptionUpdate:
		return &SubscriptionUpdate{}, nil
	case TypeSubscriptionDelete:
		return &SubscriptionDelete{}, nil
	case TypeSubscriptionOK:
		return &SubscriptionOK{}, nil
	case TypeSubscriptionListOK:
		return &SubscriptionListOK{}, nil
	case TypeSubscriptionDeleted:
		return &SubscriptionDeleted{}, nil
	case TypeChatHistory:
		return &ChatHistory{}, nil
	case TypeChatHistoryOK:
		return &ChatHistoryOK{}, nil
	case TypeMembers:
		return &Members{}, nil
	case TypeMembersOK:
		return &MembersOK{}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, t)
	}
}

// derefFrame returns the value behind a pointer-to-frame. Used so callers
// of Decode get a Frame they can type-switch on naturally without juggling
// pointers.
func derefFrame(f Frame) Frame {
	switch v := f.(type) {
	case *Hello:
		return *v
	case *Welcome:
		return *v
	case *AuthRequest:
		return *v
	case *AuthChallenge:
		return *v
	case *AuthResponse:
		return *v
	case *AuthOK:
		return *v
	case *AuthErr:
		return *v
	case *Join:
		return *v
	case *Part:
		return *v
	case *Msg:
		return *v
	case *Presence:
		return *v
	case *Ping:
		return *v
	case *Pong:
		return *v
	case *Error:
		return *v
	case *SubscriptionCreate:
		return *v
	case *SubscriptionList:
		return *v
	case *SubscriptionGet:
		return *v
	case *SubscriptionUpdate:
		return *v
	case *SubscriptionDelete:
		return *v
	case *SubscriptionOK:
		return *v
	case *SubscriptionListOK:
		return *v
	case *SubscriptionDeleted:
		return *v
	case *ChatHistory:
		return *v
	case *ChatHistoryOK:
		return *v
	case *Members:
		return *v
	case *MembersOK:
		return *v
	default:
		return f
	}
}

package proto

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 16, 20, 0, 0, 0, time.UTC)
	cases := []Frame{
		Hello{ClientName: "prm-tui", ClientVersion: "0.1.0", CapVersion: "0.1", Capabilities: []string{"presence"}},
		Welcome{ServerName: "prmd", ServerVersion: "0.1.0", CapVersion: "0.1", Capabilities: []string{"presence"}},
		AuthRequest{Method: AuthMethodPassword, Tenant: "acme", Username: "alex"},
		AuthChallenge{Salt: "abcd", Nonce: "efgh", Params: "argon2id,m=65536,t=3,p=1"},
		AuthResponse{Proof: "deadbeef"},
		AuthOK{AccountID: "acc-1", TenantID: "ten-1", AccountType: "human", DisplayName: "Alex"},
		AuthErr{Reason: "invalid_credentials"},
		Join{Channel: "general"},
		Part{Channel: "general"},
		Msg{Channel: "general", From: "acc-1", TS: now, Body: "hello world"},
		Presence{Channel: "general", Kind: PresenceJoin, AccountID: "acc-1", DisplayName: "Alex"},
		Ping{Token: "t1"},
		Pong{Token: "t1"},
		Error{Reason: "rate_limited", Detail: "too many requests"},
	}
	for _, in := range cases {
		t.Run(in.FrameType(), func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, in); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			line := buf.String()
			if !strings.HasSuffix(line, "\n") {
				t.Fatalf("Encode did not terminate with newline: %q", line)
			}
			if !strings.Contains(line, `"type":"`+in.FrameType()+`"`) {
				t.Fatalf("Encoded frame missing type %q: %s", in.FrameType(), line)
			}

			dec := NewDecoder(&buf)
			out, err := dec.Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if out.FrameType() != in.FrameType() {
				t.Fatalf("type mismatch: got %q want %q", out.FrameType(), in.FrameType())
			}
		})
	}
}

func TestDecoderEOF(t *testing.T) {
	dec := NewDecoder(strings.NewReader(""))
	_, err := dec.Decode()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestDecoderSkipsBlankLines(t *testing.T) {
	input := "\n\n" + `{"type":"ping","token":"t"}` + "\n"
	dec := NewDecoder(strings.NewReader(input))
	out, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.FrameType() != TypePing {
		t.Fatalf("got %q want ping", out.FrameType())
	}
}

func TestDecoderUnknownType(t *testing.T) {
	dec := NewDecoder(strings.NewReader(`{"type":"who_knows"}` + "\n"))
	_, err := dec.Decode()
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("expected ErrUnknownType, got %v", err)
	}
}

func TestEncodeBytesProducesSameAsEncode(t *testing.T) {
	frame := Msg{Channel: "general", Body: "hi"}
	var buf bytes.Buffer
	if err := Encode(&buf, frame); err != nil {
		t.Fatal(err)
	}
	bytesForm, err := EncodeBytes(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), bytesForm) {
		t.Fatalf("Encode and EncodeBytes differ:\n  Encode:      %q\n  EncodeBytes: %q", buf.String(), string(bytesForm))
	}
}

func TestEncodeRejectsOversizedFrame(t *testing.T) {
	big := Msg{Channel: "x", Body: strings.Repeat("A", MaxFrameSize+1)}
	var buf bytes.Buffer
	err := Encode(&buf, big)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestDecoderRejectsOversizedLine(t *testing.T) {
	line := `{"type":"msg","body":"` + strings.Repeat("A", MaxFrameSize) + `"}`
	dec := NewDecoder(strings.NewReader(line + "\n"))
	_, err := dec.Decode()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestTypeStampedAutomatically(t *testing.T) {
	// Frame constructed with Type left empty should still serialize with the
	// correct verb name. Belt-and-suspenders for callers that forget.
	in := Hello{CapVersion: "0.1"} // Type omitted on purpose
	var buf bytes.Buffer
	if err := Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"type":"hello"`) {
		t.Fatalf("Encode did not stamp type: %s", buf.String())
	}
}

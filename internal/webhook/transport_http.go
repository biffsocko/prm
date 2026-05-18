package webhook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// httpTransport is the original / default delivery path: a POST with
// JSON body, HMAC signature in the PRM-Signature header, content type
// application/json. 2xx = OK, 4xx = permanent, 5xx + network = transient.
//
// Connection reuse comes from the http.Client's transport, owned by
// the Manager and shared with this transport.
type httpTransport struct {
	client *http.Client
}

func newHTTPTransport(client *http.Client) *httpTransport {
	return &httpTransport{client: client}
}

func (httpTransport) Schemes() []string { return []string{"https", "http"} }

func (h httpTransport) Send(ctx context.Context, t Target, body []byte, sig Signature) DeliveryResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, bytes.NewReader(body))
	if err != nil {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "build request", Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("PRM-Signature", sig.Header())
	req.Header.Set("User-Agent", "prmd-webhook/0.1")

	resp, err := h.client.Do(req)
	if err != nil {
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "network", Err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return DeliveryResult{Kind: DeliveryOK, StatusDetail: fmt.Sprintf("HTTP %d", resp.StatusCode)}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: fmt.Sprintf("HTTP %d", resp.StatusCode)}
	default:
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}
}

// Close is a no-op for HTTP -- the *http.Client is owned by the
// Manager and shut down via its idle connection close.
func (httpTransport) Close() error { return nil }

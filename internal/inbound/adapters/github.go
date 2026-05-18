package adapters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
)

// GitHub adapter. Handles the common webhook event types:
//
//	push, pull_request, deployment_status, issues, release
//
// The HTTP X-GitHub-Event header tells us which event type the body is.
// Different event types nest the interesting fields differently; we
// pluck the most useful ones per type and put everything in Fields for
// bots that need more.
type GitHub struct{}

func (GitHub) Name() string { return "github" }

func (GitHub) Normalize(body []byte, headers http.Header, _ []byte) (inbound.Event, error) {
	event := headers.Get("X-GitHub-Event")
	if event == "" {
		return inbound.Event{}, fmt.Errorf("%w: X-GitHub-Event header", inbound.ErrAdapterMissing)
	}

	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		return inbound.Event{}, fmt.Errorf("%w: parse github payload: %v", inbound.ErrAdapterBadInput, err)
	}
	repoName := ""
	if repo, ok := generic["repository"].(map[string]any); ok {
		if n, ok := repo["full_name"].(string); ok {
			repoName = n
		}
	}

	summary, severity, fields := githubEventDetails(event, generic)
	if summary == "" {
		// Fall back to a generic representation.
		summary = fmt.Sprintf("github %s in %s", event, repoName)
	}
	summary = inbound.Truncate(summary, 200)

	if repoName != "" {
		fields["repository"] = repoName
	}
	fields["github_event"] = event

	return inbound.Event{
		Source:     "github",
		Service:    repoName,
		Severity:   severity,
		Summary:    summary,
		Fields:     fields,
		OccurredAt: time.Now().UTC(),
		Raw:        json.RawMessage(body),
	}, nil
}

// githubEventDetails returns a summary line + severity + sub-fields per
// event type. Unknown event types fall back to generic info with the
// raw event name.
func githubEventDetails(event string, p map[string]any) (summary, severity string, fields map[string]any) {
	fields = map[string]any{}
	severity = inbound.SeverityInfo
	switch event {
	case "push":
		who := nestedString(p, "pusher", "name")
		if who == "" {
			who = nestedString(p, "sender", "login")
		}
		ref := stringField(p, "ref")
		commits := 0
		if c, ok := p["commits"].([]any); ok {
			commits = len(c)
		}
		summary = fmt.Sprintf("%s pushed %d commit(s) to %s", who, commits, ref)
		fields["pusher"] = who
		fields["ref"] = ref
		fields["commits"] = commits
		if commits == 0 {
			severity = inbound.SeverityInfo
		}

	case "pull_request":
		action := stringField(p, "action")
		number := nestedAny(p, "pull_request", "number")
		title := nestedString(p, "pull_request", "title")
		actor := nestedString(p, "sender", "login")
		summary = fmt.Sprintf("PR #%v %s by %s: %s", number, action, actor, title)
		fields["action"] = action
		fields["number"] = number
		fields["actor"] = actor
		if action == "closed" {
			merged := nestedBool(p, "pull_request", "merged")
			if merged {
				summary = fmt.Sprintf("PR #%v merged by %s: %s", number, actor, title)
				severity = inbound.SeverityInfo
			}
		}

	case "deployment_status":
		state := nestedString(p, "deployment_status", "state")
		env := nestedString(p, "deployment", "environment")
		actor := nestedString(p, "sender", "login")
		summary = fmt.Sprintf("deployment to %s: %s (by %s)", env, state, actor)
		fields["state"] = state
		fields["environment"] = env
		fields["actor"] = actor
		switch state {
		case "failure", "error":
			severity = inbound.SeverityError
		case "success":
			severity = inbound.SeverityInfo
		default:
			severity = inbound.SeverityWarn
		}

	case "issues":
		action := stringField(p, "action")
		number := nestedAny(p, "issue", "number")
		title := nestedString(p, "issue", "title")
		actor := nestedString(p, "sender", "login")
		summary = fmt.Sprintf("issue #%v %s by %s: %s", number, action, actor, title)
		fields["action"] = action
		fields["number"] = number
		fields["actor"] = actor

	case "release":
		action := stringField(p, "action")
		tag := nestedString(p, "release", "tag_name")
		actor := nestedString(p, "sender", "login")
		summary = fmt.Sprintf("release %s %s by %s", tag, action, actor)
		fields["action"] = action
		fields["tag"] = tag
		fields["actor"] = actor
	}
	return summary, severity, fields
}

func stringField(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func nestedString(m map[string]any, k1, k2 string) string {
	sub, ok := m[k1].(map[string]any)
	if !ok {
		return ""
	}
	if v, ok := sub[k2].(string); ok {
		return v
	}
	return ""
}

func nestedBool(m map[string]any, k1, k2 string) bool {
	sub, ok := m[k1].(map[string]any)
	if !ok {
		return false
	}
	if v, ok := sub[k2].(bool); ok {
		return v
	}
	return false
}

func nestedAny(m map[string]any, k1, k2 string) any {
	sub, ok := m[k1].(map[string]any)
	if !ok {
		return nil
	}
	return sub[k2]
}

func init() { inbound.Register(GitHub{}) }

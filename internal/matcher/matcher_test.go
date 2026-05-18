package matcher_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/matcher"
)

func mustCompile(t *testing.T, j string) *matcher.Matcher {
	t.Helper()
	m, err := matcher.Compile([]byte(j))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return m
}

func TestEmptyRulesMatchEverything(t *testing.T) {
	m := mustCompile(t, ``)
	if !m.Match(matcher.Event{Body: "hello"}) {
		t.Fatal("empty matcher should match everything")
	}
	m2 := mustCompile(t, `{}`)
	if !m2.Match(matcher.Event{Body: "x"}) {
		t.Fatal("empty rules document should match everything")
	}
}

func TestRegexAnyOf(t *testing.T) {
	m := mustCompile(t, `{"any_of":[{"type":"regex","pattern":"(?i)^deploy\\b"}]}`)
	if !m.Match(matcher.Event{Body: "deploy now"}) {
		t.Errorf("'deploy now' should match (?i)^deploy")
	}
	if !m.Match(matcher.Event{Body: "Deploy production"}) {
		t.Errorf("'Deploy production' should match case-insensitive regex")
	}
	if m.Match(matcher.Event{Body: "not a deploy thing"}) {
		t.Errorf("'not a deploy thing' should not match ^deploy")
	}
}

func TestGlobAnyOf(t *testing.T) {
	m := mustCompile(t, `{"any_of":[{"type":"glob","pattern":"build #*"}]}`)
	if !m.Match(matcher.Event{Body: "build #42"}) {
		t.Errorf("glob should match")
	}
	if m.Match(matcher.Event{Body: "deploy build #42"}) {
		t.Errorf("glob is whole-body match; should NOT match this")
	}
}

func TestMentionRule(t *testing.T) {
	botID := uuid.MustParse("01010101-0000-0000-0000-000000000001")
	otherID := uuid.MustParse("02020202-0000-0000-0000-000000000002")
	m, err := matcher.CompileRules(matcher.Rules{
		AnyOf: []matcher.Rule{{Type: "mention", AccountID: botID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Match(matcher.Event{Body: "hi", Mentions: []uuid.UUID{botID}}) {
		t.Error("should match when bot is mentioned")
	}
	if m.Match(matcher.Event{Body: "hi", Mentions: []uuid.UUID{otherID}}) {
		t.Error("should NOT match when only other accounts are mentioned")
	}
	if m.Match(matcher.Event{Body: "hi"}) {
		t.Error("should NOT match when no mentions")
	}
}

func TestAnyOfIsLogicalOr(t *testing.T) {
	m := mustCompile(t, `{"any_of":[
		{"type":"regex","pattern":"^deploy"},
		{"type":"glob","pattern":"build #*"}
	]}`)
	cases := map[string]bool{
		"deploy":              true,
		"deploy now":          true,
		"build #1":            true,
		"random chat":         false,
		"talking about build": false,
	}
	for body, want := range cases {
		if got := m.Match(matcher.Event{Body: body}); got != want {
			t.Errorf("body %q: got %v want %v", body, got, want)
		}
	}
}

func TestAllOfIsLogicalAnd(t *testing.T) {
	botID := uuid.MustParse("01010101-0000-0000-0000-000000000001")
	m, err := matcher.CompileRules(matcher.Rules{
		AllOf: []matcher.Rule{
			{Type: "regex", Pattern: "(?i)urgent"},
			{Type: "mention", AccountID: botID},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Match(matcher.Event{Body: "URGENT please look", Mentions: []uuid.UUID{botID}}) {
		t.Error("urgent + bot mention should match")
	}
	if m.Match(matcher.Event{Body: "URGENT please look"}) {
		t.Error("urgent without bot mention should NOT match (all_of)")
	}
	if m.Match(matcher.Event{Body: "calm message", Mentions: []uuid.UUID{botID}}) {
		t.Error("bot mention without urgent should NOT match (all_of)")
	}
}

func TestAnyOfAndAllOfCombine(t *testing.T) {
	m := mustCompile(t, `{
		"any_of":[{"type":"regex","pattern":"deploy"}, {"type":"regex","pattern":"rollback"}],
		"all_of":[{"type":"regex","pattern":"prod"}]
	}`)
	if !m.Match(matcher.Event{Body: "deploy prod"}) {
		t.Error("deploy + prod should match")
	}
	if !m.Match(matcher.Event{Body: "rollback prod now"}) {
		t.Error("rollback + prod should match")
	}
	if m.Match(matcher.Event{Body: "deploy staging"}) {
		t.Error("deploy but not prod should NOT match")
	}
	if m.Match(matcher.Event{Body: "patch prod"}) {
		t.Error("prod but no deploy/rollback should NOT match")
	}
}

func TestCompileRejectsBadRules(t *testing.T) {
	cases := []string{
		`{"any_of":[{"type":"unknown"}]}`,
		`{"any_of":[{"type":"regex"}]}`,                  // missing pattern
		`{"any_of":[{"type":"regex","pattern":"[bad"}]}`, // unparseable
		`{"any_of":[{"type":"mention"}]}`,                // missing account_id
		`{"any_of":[{"type":"glob"}]}`,                   // missing pattern
		`{`,                                              // bad JSON
	}
	for _, c := range cases {
		_, err := matcher.Compile([]byte(c))
		if !errors.Is(err, matcher.ErrInvalidRule) {
			t.Errorf("case %q: expected ErrInvalidRule, got %v", c, err)
		}
	}
}

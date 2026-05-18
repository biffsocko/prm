// Package matcher compiles and evaluates subscription match rules.
//
// Wire-level shape (the JSON stored in storage.Subscription.MatchJSON):
//
//	{
//	  "any_of": [
//	    {"type": "mention", "account_id": "<bot uuid>"},
//	    {"type": "regex",   "pattern":    "(?i)^deploy\\b"},
//	    {"type": "glob",    "pattern":    "build #*"}
//	  ]
//	  // optionally also "all_of": [...]
//	}
//
// Semantics:
//   - A subscription matches if every rule in all_of matches AND
//     (any_of is empty OR at least one rule in any_of matches).
//   - If both lists are empty, the subscription matches everything.
//
// Compiled rules are immutable; the matcher is goroutine-safe after
// Compile.
package matcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// Rules is the on-wire / on-storage match-rules document.
type Rules struct {
	AnyOf []Rule `json:"any_of,omitempty"`
	AllOf []Rule `json:"all_of,omitempty"`
}

// Rule is a single match predicate.
type Rule struct {
	Type      string    `json:"type"`              // "mention" | "regex" | "glob"
	Pattern   string    `json:"pattern,omitempty"` // regex/glob
	AccountID uuid.UUID `json:"account_id,omitempty"`
}

// Matcher is a compiled set of rules. Construct with Compile; evaluate
// with Match. Cheap to keep around (no per-evaluation allocation on the
// hot path).
type Matcher struct {
	anyOf []compiledRule
	allOf []compiledRule
}

type compiledRule struct {
	kind      string // "mention" | "regex" | "glob"
	re        *regexp.Regexp
	pattern   string
	accountID uuid.UUID
}

// Event is the fact a subscription matcher evaluates against.
type Event struct {
	Body          string
	FromAccountID uuid.UUID
	Mentions      []uuid.UUID // any account_ids referenced in the message body (resolved by the server before calling Match)
}

// ErrInvalidRule is returned from Compile when a rule fails validation.
var ErrInvalidRule = errors.New("matcher: invalid rule")

// Compile parses Rules JSON and produces a Matcher. Rules with unsupported
// types or unparseable patterns return ErrInvalidRule.
func Compile(rulesJSON []byte) (*Matcher, error) {
	if len(rulesJSON) == 0 {
		return &Matcher{}, nil
	}
	var rs Rules
	if err := json.Unmarshal(rulesJSON, &rs); err != nil {
		return nil, fmt.Errorf("%w: parse json: %v", ErrInvalidRule, err)
	}
	return CompileRules(rs)
}

// CompileRules builds a Matcher from already-parsed Rules.
func CompileRules(rs Rules) (*Matcher, error) {
	m := &Matcher{}
	for i, r := range rs.AnyOf {
		c, err := compileOne(r)
		if err != nil {
			return nil, fmt.Errorf("any_of[%d]: %w", i, err)
		}
		m.anyOf = append(m.anyOf, c)
	}
	for i, r := range rs.AllOf {
		c, err := compileOne(r)
		if err != nil {
			return nil, fmt.Errorf("all_of[%d]: %w", i, err)
		}
		m.allOf = append(m.allOf, c)
	}
	return m, nil
}

func compileOne(r Rule) (compiledRule, error) {
	switch r.Type {
	case "mention":
		if r.AccountID == uuid.Nil {
			return compiledRule{}, fmt.Errorf("%w: mention rule needs account_id", ErrInvalidRule)
		}
		return compiledRule{kind: "mention", accountID: r.AccountID}, nil
	case "regex":
		if r.Pattern == "" {
			return compiledRule{}, fmt.Errorf("%w: regex rule needs pattern", ErrInvalidRule)
		}
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return compiledRule{}, fmt.Errorf("%w: regex compile: %v", ErrInvalidRule, err)
		}
		return compiledRule{kind: "regex", re: re, pattern: r.Pattern}, nil
	case "glob":
		if r.Pattern == "" {
			return compiledRule{}, fmt.Errorf("%w: glob rule needs pattern", ErrInvalidRule)
		}
		// Validate the glob syntax by doing a probe match.
		if _, err := path.Match(r.Pattern, ""); err != nil {
			return compiledRule{}, fmt.Errorf("%w: glob compile: %v", ErrInvalidRule, err)
		}
		return compiledRule{kind: "glob", pattern: r.Pattern}, nil
	default:
		return compiledRule{}, fmt.Errorf("%w: unknown rule type %q", ErrInvalidRule, r.Type)
	}
}

// Match returns true if the event matches the rule set.
//
// Semantics:
//   - If both anyOf and allOf are empty -> match (subscription is a "fire on every message" subscription).
//   - If allOf is non-empty, ALL rules in allOf must match.
//   - If anyOf is non-empty, AT LEAST ONE rule in anyOf must match.
//   - If both lists are non-empty, both conditions must hold.
func (m *Matcher) Match(ev Event) bool {
	if m == nil || (len(m.anyOf) == 0 && len(m.allOf) == 0) {
		return true
	}
	for _, r := range m.allOf {
		if !evalRule(r, ev) {
			return false
		}
	}
	if len(m.anyOf) == 0 {
		return true
	}
	for _, r := range m.anyOf {
		if evalRule(r, ev) {
			return true
		}
	}
	return false
}

func evalRule(r compiledRule, ev Event) bool {
	switch r.kind {
	case "mention":
		for _, m := range ev.Mentions {
			if m == r.accountID {
				return true
			}
		}
		return false
	case "regex":
		return r.re.MatchString(ev.Body)
	case "glob":
		// path.Match is line-based, not multiline; we match against the
		// trimmed body. Multi-pattern globs would need a Glob library.
		match, _ := path.Match(r.pattern, strings.TrimSpace(ev.Body))
		return match
	default:
		return false
	}
}

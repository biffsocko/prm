package rest

import "github.com/biffsocko/prm/internal/matcher"

// validateMatchJSON runs the matcher's compile to validate user-supplied
// match-rule JSON without keeping the compiled matcher around. Returns
// matcher.ErrInvalidRule-wrapped errors so REST handlers can surface a
// 400 with a useful message.
func validateMatchJSON(raw []byte) error {
	_, err := matcher.Compile(raw)
	return err
}

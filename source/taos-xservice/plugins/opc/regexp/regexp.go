package regexp

import (
	"regexp"

	"github.com/dlclark/regexp2"
)

type Regexp interface {
	MatchString(string) bool
}

func Compile(pattern string) (Regexp, error) {
	reg1, err := regexp.Compile(pattern)
	if err == nil {
		return reg1, nil
	}
	return newRegexp(pattern)
}

type Regexp2Wrapper struct {
	reg2 *regexp2.Regexp
}

func newRegexp(pattern string) (*Regexp2Wrapper, error) {
	reg2, err := regexp2.Compile(pattern, regexp2.None)
	if err != nil {
		return nil, err
	}
	return &Regexp2Wrapper{reg2: reg2}, nil

}

func (r *Regexp2Wrapper) MatchString(s string) bool {
	match, err := r.reg2.MatchString(s)
	return err == nil && match
}

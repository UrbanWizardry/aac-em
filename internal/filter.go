package emulator

import (
	"fmt"
	"regexp"
	"strings"
)

type Filter interface {
	Apply(string) bool
}

// nullFilter always returns true
type nullFilter struct{}

var _ Filter = (*nullFilter)(nil)

func (nf nullFilter) Apply(string) bool {
	return true
}

type filter struct {
	regexps []regexp.Regexp
}

var _ Filter = (*filter)(nil)

func (f filter) Apply(s string) bool {
	for _, r := range f.regexps {
		if r.Match([]byte(s)) {
			return true
		}
	}

	return false
}

func newFilter(filterString string) (filter, error) {
	f := filter{
		regexps: []regexp.Regexp{},
	}

	if strings.Contains(filterString, "*") {
		// You can't mix-and-match * and ,
		if strings.Contains(filterString, ",") {
			return filter{}, fmt.Errorf("csv filters cannot use *")
		}

		// The * MUST be on the end
		if strings.Index(filterString, "*") != len(filterString)-1 {
			return filter{}, fmt.Errorf("filters can only use * at the end")
		}

		r, err := regexp.Compile(fmt.Sprintf("%s./*", strings.Trim(filterString, "*")))
		if err != nil {
			return filter{}, fmt.Errorf("error compiling filter regexp")
		}
		f.regexps = append(f.regexps, *r)

		return f, nil
	}

	// No wildcards, so it's either a single or a CSV, both handled the same way
	subs := strings.Split(filterString, ",")
	for _, sub := range subs {
		// Allow for foolish callers using whitespace, we're generous
		trimSub := strings.Trim(sub, " ")
		if strings.Contains(trimSub, "*") {

		}

		// Must be an exact match
		r, err := regexp.Compile(trimSub)
		if err != nil {
			return filter{}, fmt.Errorf("error compiling filter regexp")
		}
		f.regexps = append(f.regexps, *r)
	}

	return f, nil
}

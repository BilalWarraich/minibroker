package broker

import (
	"errors"
	"strings"
)

// Subjects are dot separated, like "orders.eu.created". Subscriptions can use
// wildcards the same way NATS does:
//
//	*  matches exactly one token     orders.*  -> orders.created, not orders.eu.created
//	>  matches one or more tokens    orders.>  -> orders.created, orders.eu.created
//	   (only allowed as the last token)

var (
	ErrBadPattern    = errors.New("bad subscription pattern")
	ErrWildcardInPub = errors.New("can't publish to a subject with wildcards")
)

func isPattern(s string) bool {
	return strings.ContainsAny(s, "*>")
}

func validPattern(p string) bool {
	toks := strings.Split(p, ".")
	for i, t := range toks {
		switch {
		case t == "":
			return false
		case t == ">" && i != len(toks)-1:
			return false
		case t != "*" && t != ">" && strings.ContainsAny(t, "*>"):
			// "ord*" isn't a thing, wildcards have to be a whole token
			return false
		}
	}
	return true
}

// match assumes pattern was already validated
func match(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i // needs at least one token left
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

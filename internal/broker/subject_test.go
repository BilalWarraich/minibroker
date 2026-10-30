package broker

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.eu.created", false},
		{"orders.*", "orders", false},
		{"orders.*", "payments.created", false},
		{"*.created", "orders.created", true},
		{"*.*", "a.b", true},
		{"*.*", "a", false},
		{"orders.>", "orders.created", true},
		{"orders.>", "orders.eu.created", true},
		{"orders.>", "orders", false}, // > needs at least one more token
		{">", "anything.at.all", true},
		{"orders.*.created", "orders.eu.created", true},
		{"orders.*.created", "orders.eu.paid", false},
		{"orders.*.>", "orders.eu.created.v2", true},
	}
	for _, c := range cases {
		t.Run(c.pattern+" vs "+c.subject, func(t *testing.T) {
			if got := match(c.pattern, c.subject); got != c.want {
				t.Fatalf("match(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
			}
		})
	}
}

func TestValidPattern(t *testing.T) {
	cases := map[string]bool{
		"orders.*":   true,
		"orders.>":   true,
		"*.created":  true,
		">":          true,
		"orders.>.x": false, // > has to be last
		"ord*":       false,
		"orders..*":  false,
		"orders.*.":  false,
	}
	for p, want := range cases {
		if got := validPattern(p); got != want {
			t.Errorf("validPattern(%q) = %v, want %v", p, got, want)
		}
	}
}

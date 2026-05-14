package probe

import (
	"context"
	"errors"
	"testing"
)

func TestStatusString(t *testing.T) {
	t.Parallel()
	cases := map[Status]string{
		StatusUnknown: "unknown",
		StatusUp:      "up",
		StatusDown:    "down",
		StatusTimeout: "timeout",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("Status(%d).String() = %q, want %q", s, got, want)
		}
	}
}

func TestStatusOK(t *testing.T) {
	t.Parallel()
	if !StatusUp.OK() {
		t.Error("StatusUp.OK() must be true")
	}
	for _, s := range []Status{StatusUnknown, StatusDown, StatusTimeout} {
		if s.OK() {
			t.Errorf("%v.OK() must be false", s)
		}
	}
}

func TestFunc(t *testing.T) {
	t.Parallel()
	p := Func(func(context.Context) Status { return StatusUp })
	if p.Check(context.Background()) != StatusUp {
		t.Fatal("Func adapter broken")
	}
}

func TestFromError(t *testing.T) {
	t.Parallel()
	ok := FromError(func(context.Context) error { return nil })
	if ok.Check(context.Background()) != StatusUp {
		t.Fatal("nil error must yield StatusUp")
	}
	bad := FromError(func(context.Context) error { return errors.New("x") })
	if bad.Check(context.Background()) != StatusDown {
		t.Fatal("non-nil error must yield StatusDown")
	}
}

func TestBool(t *testing.T) {
	t.Parallel()
	b := NewBool()
	if b.Check(context.Background()) != StatusDown {
		t.Fatal("zero-value Bool must be StatusDown")
	}
	b.Set(true)
	if b.Check(context.Background()) != StatusUp {
		t.Fatal("Set(true) must yield StatusUp")
	}
	if !b.Get() {
		t.Fatal("Get must reflect Set")
	}
	b.Set(false)
	if b.Check(context.Background()) != StatusDown {
		t.Fatal("Set(false) must yield StatusDown")
	}
}

func TestNamed(t *testing.T) {
	t.Parallel()
	n := WithName("db", NewBool())
	if n.Name() != "db" {
		t.Fatal("Name mismatch")
	}
	if n.Check(context.Background()) != StatusDown {
		t.Fatal("Named must delegate to underlying probe")
	}
}

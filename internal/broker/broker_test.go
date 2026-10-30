package broker

import (
	"sync"
	"testing"
	"time"
)

func recv(t *testing.T, s *Subscription) Message {
	t.Helper()
	select {
	case m, ok := <-s.C():
		if !ok {
			t.Fatal("channel closed")
		}
		return m
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
	return Message{}
}

func TestPubSub(t *testing.T) {
	b := New(Options{})
	s1, _ := b.Subscribe("news")
	s2, _ := b.Subscribe("news")
	other, _ := b.Subscribe("sports")

	_, delivered, _, err := b.Publish("news", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if delivered != 2 {
		t.Fatalf("delivered=%d", delivered)
	}

	if string(recv(t, s1).Data) != "hello" || string(recv(t, s2).Data) != "hello" {
		t.Fatal("wrong data")
	}
	select {
	case <-other.C():
		t.Fatal("sports subscriber should not get news")
	default:
	}
}

func TestSlowSubscriberGetsKicked(t *testing.T) {
	b := New(Options{BufferSize: 2, MaxDrops: 3})
	slow, _ := b.Subscribe("t")
	fast, _ := b.Subscribe("t")

	go func() {
		for range fast.C() {
		}
	}()

	// 2 fill the buffer, 3 more get dropped -> kicked
	for i := 0; i < 5; i++ {
		b.Publish("t", []byte("x"))
		time.Sleep(time.Millisecond) // give fast a chance to read
	}

	// drain what's buffered, then channel should be closed
	n := 0
	for range slow.C() {
		n++
	}
	if n != 2 {
		t.Fatalf("expected 2 buffered msgs, got %d", n)
	}
	if !slow.Kicked() {
		t.Fatal("expected slow sub to be kicked")
	}
	if fast.Kicked() {
		t.Fatal("fast sub should still be around")
	}
}

func TestCloseSubscription(t *testing.T) {
	b := New(Options{})
	s, _ := b.Subscribe("t")
	s.Close()
	s.Close() // shouldn't panic

	if _, ok := <-s.C(); ok {
		t.Fatal("expected closed channel")
	}
	if len(b.Topics()) != 0 {
		t.Fatal("topic should be cleaned up")
	}
}

func TestBrokerClose(t *testing.T) {
	b := New(Options{})
	s, _ := b.Subscribe("t")
	b.Close()

	if _, ok := <-s.C(); ok {
		t.Fatal("expected closed")
	}
	if _, err := b.Subscribe("t"); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
	if _, _, _, err := b.Publish("t", nil); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
}

// mostly here to give the race detector something to chew on
func TestConcurrentPublishSubscribe(t *testing.T) {
	b := New(Options{BufferSize: 8, MaxDrops: 1000})
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := b.Subscribe("load")
			if err != nil {
				return
			}
			n := 0
			for range s.C() {
				n++
				if n == 20 {
					s.Close()
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.Publish("load", []byte("x"))
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	b.Close()
	wg.Wait()
}

func TestWildcardSubscribe(t *testing.T) {
	b := New(Options{})
	one, _ := b.Subscribe("orders.*")
	all, _ := b.Subscribe("orders.>")
	exact, _ := b.Subscribe("orders.created")

	_, delivered, _, err := b.Publish("orders.created", []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if delivered != 3 {
		t.Fatalf("orders.created should reach all 3 subs, delivered=%d", delivered)
	}
	recv(t, one)
	recv(t, all)
	recv(t, exact)

	// two levels deep: only the > subscription matches
	_, delivered, _, _ = b.Publish("orders.eu.created", []byte("b"))
	if delivered != 1 {
		t.Fatalf("delivered=%d, want 1", delivered)
	}
	if string(recv(t, all).Data) != "b" {
		t.Fatal("wrong message")
	}
	select {
	case <-one.C():
		t.Fatal("orders.* shouldn't match orders.eu.created")
	default:
	}
}

func TestWildcardErrors(t *testing.T) {
	b := New(Options{})
	if _, err := b.Subscribe("orders.>.x"); err != ErrBadPattern {
		t.Fatalf("got %v", err)
	}
	if _, _, _, err := b.Publish("orders.*", nil); err != ErrWildcardInPub {
		t.Fatalf("got %v", err)
	}
}

func TestWildcardCloseCleansUp(t *testing.T) {
	b := New(Options{})
	s, _ := b.Subscribe("orders.*")
	if len(b.Topics()) != 1 {
		t.Fatal("pattern should show up in Topics")
	}
	s.Close()
	if len(b.Topics()) != 0 {
		t.Fatal("pattern should be gone after Close")
	}
}

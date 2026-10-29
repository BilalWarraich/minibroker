// Package broker is the in-memory pub/sub part, no grpc in here.
package broker

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("broker closed")

type Message struct {
	ID          uint64
	Topic       string
	Data        []byte
	PublishedAt time.Time
}

type Options struct {
	// buffer per subscriber
	BufferSize int
	// how many messages in a row a subscriber can miss before we kick it
	MaxDrops int
}

// Locking:
//   - b.mu guards the topics map and closed. Publish only takes it for reading,
//     so publishers on different topics don't block each other.
//   - topic.mu guards that topic's subscribers (and their drops/kicked).
//   - always lock b.mu before topic.mu, never the other way around.
type Broker struct {
	opts Options

	mu     sync.RWMutex
	topics map[string]*topic
	closed bool

	nextSubID atomic.Uint64
	nextMsgID atomic.Uint64
}

type topic struct {
	mu   sync.Mutex
	subs map[uint64]*Subscription
}

func New(opts Options) *Broker {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64
	}
	if opts.MaxDrops <= 0 {
		opts.MaxDrops = 100
	}
	return &Broker{
		opts:   opts,
		topics: make(map[string]*topic),
	}
}

type Subscription struct {
	id     uint64
	topic  string
	t      *topic
	ch     chan Message
	b      *Broker
	drops  int // consecutive, guarded by t.mu
	kicked bool
	once   sync.Once
}

// C is closed when the subscription ends, either because Close was called,
// the broker shut down, or the subscriber was too slow.
func (s *Subscription) C() <-chan Message { return s.ch }

// Kicked reports whether we got dropped for being slow.
// Only meaningful after C is closed.
func (s *Subscription) Kicked() bool {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	return s.kicked
}

func (s *Subscription) Close() {
	b := s.b
	b.mu.Lock()
	defer b.mu.Unlock()

	s.t.mu.Lock()
	s.t.remove(s)
	empty := len(s.t.subs) == 0
	s.t.mu.Unlock()

	// only drop the topic if it's still the one we belong to
	if empty && b.topics[s.topic] == s.t {
		delete(b.topics, s.topic)
	}
}

func (b *Broker) Subscribe(name string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	t := b.topics[name]
	if t == nil {
		t = &topic{subs: make(map[uint64]*Subscription)}
		b.topics[name] = t
	}
	s := &Subscription{
		id:    b.nextSubID.Add(1),
		topic: name,
		t:     t,
		ch:    make(chan Message, b.opts.BufferSize),
		b:     b,
	}
	t.mu.Lock()
	t.subs[s.id] = s
	t.mu.Unlock()
	return s, nil
}

// remove needs t.mu held
func (t *topic) remove(s *Subscription) {
	delete(t.subs, s.id)
	s.once.Do(func() { close(s.ch) })
}

// Publish never blocks on a slow subscriber. If a subscriber's buffer is full the
// message is dropped for that subscriber only.
func (b *Broker) Publish(name string, data []byte) (msg Message, delivered, dropped int, err error) {
	msg = Message{
		ID:          b.nextMsgID.Add(1),
		Topic:       name,
		Data:        data,
		PublishedAt: time.Now(),
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return msg, 0, 0, ErrClosed
	}
	t := b.topics[name]
	if t == nil {
		return msg, 0, 0, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.subs {
		select {
		case s.ch <- msg:
			s.drops = 0
			delivered++
		default:
			s.drops++
			dropped++
			if s.drops >= b.opts.MaxDrops {
				// deleting from a map while ranging over it is fine in go
				s.kicked = true
				t.remove(s)
			}
		}
	}
	// a kicked-empty topic stays in the map until the next Close/Subscribe cleanup,
	// Topics() just skips it. not worth taking the write lock here for that.
	return msg, delivered, dropped, nil
}

type TopicInfo struct {
	Name        string
	Subscribers int
}

func (b *Broker) Topics() []TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TopicInfo, 0, len(b.topics))
	for name, t := range b.topics {
		t.mu.Lock()
		n := len(t.subs)
		t.mu.Unlock()
		if n > 0 {
			out = append(out, TopicInfo{name, n})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Close ends all subscriptions.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for name, t := range b.topics {
		t.mu.Lock()
		for _, s := range t.subs {
			t.remove(s)
		}
		t.mu.Unlock()
		delete(b.topics, name)
	}
}

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

type Broker struct {
	opts Options

	mu     sync.RWMutex
	topics map[string]map[uint64]*Subscription
	closed bool

	nextSubID atomic.Uint64
	nextMsgID atomic.Uint64
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
		topics: make(map[string]map[uint64]*Subscription),
	}
}

type Subscription struct {
	id     uint64
	topic  string
	ch     chan Message
	b      *Broker
	drops  int // consecutive, guarded by b.mu (see Publish)
	kicked bool
	once   sync.Once
}

// C is closed when the subscription ends, either because Close was called,
// the broker shut down, or the subscriber was too slow.
func (s *Subscription) C() <-chan Message { return s.ch }

// Kicked reports whether we got dropped for being slow.
// Only meaningful after C is closed.
func (s *Subscription) Kicked() bool {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()
	return s.kicked
}

func (s *Subscription) Close() {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.remove(s)
}

func (b *Broker) Subscribe(topic string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	s := &Subscription{
		id:    b.nextSubID.Add(1),
		topic: topic,
		ch:    make(chan Message, b.opts.BufferSize),
		b:     b,
	}
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[uint64]*Subscription)
	}
	b.topics[topic][s.id] = s
	return s, nil
}

// remove needs b.mu held for writing
func (b *Broker) remove(s *Subscription) {
	subs := b.topics[s.topic]
	if subs != nil {
		delete(subs, s.id)
		if len(subs) == 0 {
			delete(b.topics, s.topic)
		}
	}
	s.once.Do(func() { close(s.ch) })
}

// Publish never blocks on a slow subscriber. If a subscriber's buffer is full the
// message is dropped for that subscriber only.
func (b *Broker) Publish(topic string, data []byte) (msg Message, delivered, dropped int, err error) {
	msg = Message{
		ID:          b.nextMsgID.Add(1),
		Topic:       topic,
		Data:        data,
		PublishedAt: time.Now(),
	}

	// Taking the write lock here is a bit heavy, but we mutate drops and may kick
	// subscribers. Good enough for now, could shard per topic if it becomes a problem.
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return msg, 0, 0, ErrClosed
	}

	var toKick []*Subscription
	for _, s := range b.topics[topic] {
		select {
		case s.ch <- msg:
			s.drops = 0
			delivered++
		default:
			s.drops++
			dropped++
			if s.drops >= b.opts.MaxDrops {
				toKick = append(toKick, s)
			}
		}
	}
	for _, s := range toKick {
		s.kicked = true
		b.remove(s)
	}
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
	for name, subs := range b.topics {
		out = append(out, TopicInfo{name, len(subs)})
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
	for _, subs := range b.topics {
		for _, s := range subs {
			b.remove(s)
		}
	}
}

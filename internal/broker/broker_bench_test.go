package broker

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// drain keeps subscribers reading so publishes measure the broker,
// not a pile of full buffers
func benchBroker(b *testing.B, topics, subsPerTopic int) (*Broker, []string) {
	b.Helper()
	br := New(Options{BufferSize: 1024, MaxDrops: 1 << 30})
	names := make([]string, topics)
	for i := range names {
		names[i] = fmt.Sprintf("topic-%d", i)
		for j := 0; j < subsPerTopic; j++ {
			s, err := br.Subscribe(names[i])
			if err != nil {
				b.Fatal(err)
			}
			go func() {
				for range s.C() {
				}
			}()
		}
	}
	b.Cleanup(br.Close)
	return br, names
}

// many publishers spread over many topics, this is where one global lock hurts
func BenchmarkPublishManyTopics(b *testing.B) {
	br, topics := benchBroker(b, 16, 2)
	payload := []byte("hello")
	var n atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// each goroutine sticks to its own topic
		topic := topics[int(n.Add(1))%len(topics)]
		for pb.Next() {
			if _, _, _, err := br.Publish(topic, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// everyone on the same topic, sharding can't help here
func BenchmarkPublishOneTopic(b *testing.B) {
	br, topics := benchBroker(b, 1, 2)
	payload := []byte("hello")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, _, err := br.Publish(topics[0], payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

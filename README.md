# minibroker

A tiny pub/sub message broker over gRPC. Not trying to compete with NATS or Kafka,
I just wanted to understand how fan-out and backpressure work by building one.

Publishers send messages to a topic, subscribers get them through a gRPC server stream.

```
  pub ──► Publish(topic) ──► broker ──┬──► sub chan (buffered) ──► Subscribe stream ──► client A
                                      ├──► sub chan (buffered) ──► Subscribe stream ──► client B
                                      └──► sub chan (full!)    ──► dropped, kicked after N in a row
```

## How it works

- Each subscriber gets its own buffered channel. Each `Subscribe` RPC runs in its own goroutine
  (grpc gives you that for free) and just reads from the channel and calls `stream.Send`.
- `Publish` never blocks. If a subscriber's buffer is full the message is dropped *for that subscriber*,
  so one slow consumer can't stall everyone else.
- If a subscriber misses `-max-drops` messages in a row it gets disconnected with `RESOURCE_EXHAUSTED`.
- When the client disconnects, the stream ctx is cancelled and the subscription is cleaned up.
- On SIGTERM: health goes to NOT_SERVING, broker closes (which ends all streams), then `GracefulStop`.

Also has:
- logging + bearer token auth interceptors (unary and stream)
- grpc health service and reflection, so `grpcurl` works
- tests using `bufconn` (no real network needed)

No persistence, no acks, at-most-once delivery. Messages published to a topic with no subscribers are just gone.

## Running

```
go run ./cmd/minibroker
```

In another terminal:

```
go run ./cmd/mbctl sub chat
```

And another one:

```
go run ./cmd/mbctl pub chat hello there
go run ./cmd/mbctl topics
```

With auth:

```
MINIBROKER_TOKEN=secret go run ./cmd/minibroker
MINIBROKER_TOKEN=secret go run ./cmd/mbctl topics
```

grpcurl:

```
grpcurl -plaintext -d '{"topic":"chat","data":"aGk="}' localhost:50051 minibroker.v1.Broker/Publish
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

Flags: `-addr` (`:50051`), `-buffer` per-subscriber buffer (64), `-max-drops` (100).

## Regenerating the proto code

Generated code is committed so you don't need protoc just to build. If you change `proto/broker.proto`:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto
```

## Tests

```
make test
```

## Maybe later

- wildcard topics (`orders.*`)
- replay last N messages for new subscribers
- metrics (msgs/sec, drops per topic)
- shard the lock per topic, right now Publish takes a global lock

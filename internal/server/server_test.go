package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/BilalWarraich/minibroker/gen/brokerpb"
	"github.com/BilalWarraich/minibroker/internal/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func startServer(t *testing.T, b *broker.Broker, token string) brokerpb.BrokerClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)

	auth := Auth{Token: token}
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.Unary),
		grpc.ChainStreamInterceptor(auth.Stream),
	)
	brokerpb.RegisterBrokerServer(gs, New(b))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return brokerpb.NewBrokerClient(conn)
}

// waitForSubs polls until the topic has n subscribers, since Subscribe returning
// on the client doesn't mean the server has registered it yet
func waitForSubs(t *testing.T, c brokerpb.BrokerClient, topic string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.ListTopics(context.Background(), &brokerpb.ListTopicsRequest{})
		if err == nil {
			for _, tp := range resp.Topics {
				if tp.Name == topic && int(tp.Subscribers) == n {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("topic %s never got %d subscribers", topic, n)
}

func TestPublishSubscribe(t *testing.T) {
	c := startServer(t, broker.New(broker.Options{}), "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.Subscribe(ctx, &brokerpb.SubscribeRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	waitForSubs(t, c, "orders", 1)

	resp, err := c.Publish(ctx, &brokerpb.PublishRequest{Topic: "orders", Data: []byte("order #1")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Delivered != 1 {
		t.Fatalf("delivered = %d", resp.Delivered)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != "order #1" || msg.Id != resp.Id {
		t.Fatalf("got %+v", msg)
	}
}

func TestValidation(t *testing.T) {
	c := startServer(t, broker.New(broker.Options{}), "")
	_, err := c.Publish(context.Background(), &brokerpb.PublishRequest{Topic: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v", err)
	}
}

func TestAuth(t *testing.T) {
	c := startServer(t, broker.New(broker.Options{}), "s3cret")

	_, err := c.ListTopics(context.Background(), &brokerpb.ListTopicsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer s3cret")
	if _, err := c.ListTopics(ctx, &brokerpb.ListTopicsRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriberDisconnectCleansUp(t *testing.T) {
	c := startServer(t, broker.New(broker.Options{}), "")

	ctx, cancel := context.WithCancel(context.Background())
	_, err := c.Subscribe(ctx, &brokerpb.SubscribeRequest{Topic: "tmp"})
	if err != nil {
		t.Fatal(err)
	}
	waitForSubs(t, c, "tmp", 1)
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, _ := c.ListTopics(context.Background(), &brokerpb.ListTopicsRequest{})
		if len(resp.GetTopics()) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("subscription was not cleaned up after client cancelled")
}

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BilalWarraich/minibroker/gen/brokerpb"
	"github.com/BilalWarraich/minibroker/internal/broker"
	"github.com/BilalWarraich/minibroker/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	buf := flag.Int("buffer", 64, "per-subscriber buffer size")
	maxDrops := flag.Int("max-drops", 100, "kick a subscriber after this many dropped messages in a row")
	flag.Parse()

	token := os.Getenv("MINIBROKER_TOKEN")

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	b := broker.New(broker.Options{BufferSize: *buf, MaxDrops: *maxDrops})
	auth := server.Auth{Token: token}

	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(server.LoggingUnary, auth.Unary),
		grpc.ChainStreamInterceptor(server.LoggingStream, auth.Stream),
	)
	brokerpb.RegisterBrokerServer(gs, server.New(b))

	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs) // so grpcurl works

	go func() {
		if token == "" {
			log.Println("warning: MINIBROKER_TOKEN not set, auth disabled")
		}
		log.Printf("listening on %s", *addr)
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Println("shutting down")
	hs.Shutdown()
	// closing the broker ends all Subscribe streams, otherwise GracefulStop
	// would wait on them forever
	b.Close()

	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Println("graceful stop timed out, forcing")
		gs.Stop()
	}
}

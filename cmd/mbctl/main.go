// mbctl is a small cli for poking at minibroker.
//
//	mbctl pub <topic> <message>
//	mbctl sub <topic>
//	mbctl topics
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/BilalWarraich/minibroker/gen/brokerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "broker address")
	useTLS := flag.Bool("tls", false, "use tls (needed for hosted brokers, e.g. something.koyeb.app:443)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: mbctl [-addr host:port] pub <topic> <msg> | sub <topic> | topics")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	creds := insecure.NewCredentials()
	if *useTLS {
		creds = credentials.NewTLS(&tls.Config{})
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := brokerpb.NewBrokerClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if tok := os.Getenv("MINIBROKER_TOKEN"); tok != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
	}

	switch args[0] {
	case "pub":
		if len(args) < 3 {
			flag.Usage()
			os.Exit(2)
		}
		resp, err := client.Publish(ctx, &brokerpb.PublishRequest{
			Topic: args[1],
			Data:  []byte(strings.Join(args[2:], " ")),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("published #%d (delivered=%d dropped=%d)\n", resp.Id, resp.Delivered, resp.Dropped)

	case "sub":
		if len(args) < 2 {
			flag.Usage()
			os.Exit(2)
		}
		stream, err := client.Subscribe(ctx, &brokerpb.SubscribeRequest{Topic: args[1]})
		if err != nil {
			log.Fatal(err)
		}
		for {
			m, err := stream.Recv()
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			ts := time.UnixMilli(m.PublishedAtUnixMs).Format("15:04:05.000")
			fmt.Printf("[%s] #%d %s: %s\n", ts, m.Id, m.Topic, m.Data)
		}

	case "topics":
		resp, err := client.ListTopics(ctx, &brokerpb.ListTopicsRequest{})
		if err != nil {
			log.Fatal(err)
		}
		if len(resp.Topics) == 0 {
			fmt.Println("no active topics")
		}
		for _, t := range resp.Topics {
			fmt.Printf("%-20s %d subscribers\n", t.Name, t.Subscribers)
		}

	default:
		flag.Usage()
		os.Exit(2)
	}
}

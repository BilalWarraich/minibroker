package server

import (
	"context"
	"errors"

	"github.com/BilalWarraich/minibroker/gen/brokerpb"
	"github.com/BilalWarraich/minibroker/internal/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxMsgSize = 256 << 10 // 256kb, this isn't meant for big payloads

type Server struct {
	brokerpb.UnimplementedBrokerServer
	b *broker.Broker
}

func New(b *broker.Broker) *Server {
	return &Server{b: b}
}

func (s *Server) Publish(ctx context.Context, req *brokerpb.PublishRequest) (*brokerpb.PublishResponse, error) {
	if req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if len(req.GetData()) > maxMsgSize {
		return nil, status.Error(codes.InvalidArgument, "message too big")
	}

	msg, delivered, dropped, err := s.b.Publish(req.GetTopic(), req.GetData())
	if errors.Is(err, broker.ErrClosed) {
		return nil, status.Error(codes.Unavailable, "shutting down")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &brokerpb.PublishResponse{
		Id:        msg.ID,
		Delivered: int32(delivered),
		Dropped:   int32(dropped),
	}, nil
}

func (s *Server) Subscribe(req *brokerpb.SubscribeRequest, stream brokerpb.Broker_SubscribeServer) error {
	if req.GetTopic() == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	sub, err := s.b.Subscribe(req.GetTopic())
	if err != nil {
		return status.Error(codes.Unavailable, "shutting down")
	}
	defer sub.Close()

	for {
		select {
		case <-stream.Context().Done():
			// client went away
			return nil
		case m, ok := <-sub.C():
			if !ok {
				if sub.Kicked() {
					return status.Error(codes.ResourceExhausted, "subscriber too slow, disconnected")
				}
				return status.Error(codes.Unavailable, "broker shutting down")
			}
			err := stream.Send(&brokerpb.Message{
				Id:                m.ID,
				Topic:             m.Topic,
				Data:              m.Data,
				PublishedAtUnixMs: m.PublishedAt.UnixMilli(),
			})
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) ListTopics(ctx context.Context, _ *brokerpb.ListTopicsRequest) (*brokerpb.ListTopicsResponse, error) {
	resp := &brokerpb.ListTopicsResponse{}
	for _, t := range s.b.Topics() {
		resp.Topics = append(resp.Topics, &brokerpb.TopicInfo{
			Name:        t.Name,
			Subscribers: int32(t.Subscribers),
		})
	}
	return resp, nil
}

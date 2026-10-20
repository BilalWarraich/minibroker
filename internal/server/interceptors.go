package server

import (
	"context"
	"crypto/subtle"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func LoggingUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("%s %s %v", info.FullMethod, status.Code(err), time.Since(start).Round(time.Microsecond))
	return resp, err
}

func LoggingStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	log.Printf("%s stream opened", info.FullMethod)
	err := handler(srv, ss)
	log.Printf("%s stream closed %s after %v", info.FullMethod, status.Code(err), time.Since(start).Round(time.Millisecond))
	return err
}

// Auth checks for "authorization: Bearer <token>". If token is empty, auth is off.
type Auth struct {
	Token string
}

func (a Auth) check(ctx context.Context) error {
	if a.Token == "" {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return status.Error(codes.Unauthenticated, "missing token")
	}
	got := strings.TrimPrefix(vals[0], "Bearer ")
	if subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) != 1 {
		return status.Error(codes.Unauthenticated, "bad token")
	}
	return nil
}

func (a Auth) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := a.check(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (a Auth) Stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := a.check(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

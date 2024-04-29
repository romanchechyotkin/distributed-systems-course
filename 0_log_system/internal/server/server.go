package server

import (
	"context"

	api "log_system/api/v1"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(offset uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(sub, obj, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (server *grpc.Server, err error) {
	opts = append(opts,
		grpc.StreamInterceptor(grpcmiddleware.ChainStreamServer(grpcauth.StreamServerInterceptor(authenticate))),
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(grpcauth.UnaryServerInterceptor(authenticate))),
	)

	gsrv := grpc.NewServer(opts...)

	srv, err := newServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)

	return gsrv, nil
}

func newServer(cfg *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: cfg,
	}

	return srv, nil
}

func (srv *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := srv.Authorizer.Authorize(subjectFromContext(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}

	off, err := srv.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, nil
}

func (srv *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := srv.Authorizer.Authorize(subjectFromContext(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}

	record, err := srv.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil
}

func (srv *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := srv.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(resp); err != nil {
			return err
		}
	}
}

func (srv *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			resp, err := srv.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(resp); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "could not find peer in context").Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName

	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subjectFromContext(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

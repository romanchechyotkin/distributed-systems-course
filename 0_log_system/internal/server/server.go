package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"

	api "log_system/api/v1"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("offset out of range: %d", e.Offset))
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", e.Offset)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(offset uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config) (server *grpc.Server, err error) {
	gsrv := grpc.NewServer()

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
	off, err := srv.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, nil
}

func (srv *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
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
			case ErrOffsetOutOfRange:
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

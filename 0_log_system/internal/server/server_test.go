package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "log_system/api/v1"
	"log_system/config/appconfig"
	"log_system/config/segmentconfig"
	"log_system/internal/auth"
	"log_system/internal/config"
	"log_system/internal/log"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			fn(t, rootClient, nobodyClient, cfg)
			teardown()
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	listen, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		clientTLSConfig, err := config.SetupTLS(&config.TLSConfig{
			CAFile:   config.CAFile,
			CertFile: crtPath,
			KeyFile:  keyPath,
			Server:   false,
		})
		require.NoError(t, err)

		clientCredentials := credentials.NewTLS(clientTLSConfig)
		clientOpts := []grpc.DialOption{grpc.WithTransportCredentials(clientCredentials)}

		conn, err := grpc.Dial(listen.Addr().String(), clientOpts...)
		require.NoError(t, err)

		client := api.NewLogClient(conn)

		return conn, client, clientOpts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLS(&config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listen.Addr().String(),
		Server:        true,
	})

	serverCredentials := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	commitLog, err := log.New(dir, &appconfig.AppConfig{
		Port:    0,
		Segment: &segmentconfig.SegmentCofig{},
	})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog:  commitLog,
		Authorizer: auth.New(config.ACLModelFile, config.ACLPolicyFile),
	}

	if fn != nil {
		fn(cfg)
	}

	srv, err := NewGRPCServer(cfg, grpc.Creds(serverCredentials))
	require.NoError(t, err)

	go srv.Serve(listen)

	return rootClient, nobodyClient, cfg, func() {
		srv.Stop()
		rootConn.Close()
		nobodyConn.Close()
		listen.Close()
		commitLog.Remove()
	}
}

func testProduceConsume(t *testing.T, rootClient, nobodyClient api.LogClient, cfg *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := rootClient.Produce(ctx, &api.ProduceRequest{
		Record: want,
	})
	require.NoError(t, err)

	off := produce.Offset

	consume, err := rootClient.Consume(ctx, &api.ConsumeRequest{Offset: off})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)

	produce, err = nobodyClient.Produce(ctx, &api.ProduceRequest{
		Record: want,
	})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	consume, err = nobodyClient.Consume(ctx, &api.ConsumeRequest{Offset: off})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func testProduceConsumeStream(t *testing.T, rootClient, nobodyClient api.LogClient, cfg *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first"),
			Offset: 0,
		},
		{
			Value:  []byte("second"),
			Offset: 1,
		},
	}

	{
		stream, err := rootClient.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			produceResponse, err := stream.Recv()
			require.NoError(t, err)

			if produceResponse.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d",
					produceResponse.Offset,
					offset,
				)
			}
		}
	}

	{
		stream, err := rootClient.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for offset, record := range records {
			consumeResponse, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, consumeResponse.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(offset),
			})
		}
	}
}

func testConsumePastBoundary(t *testing.T, rootClient, nobodyClient api.LogClient, cfg *Config) {
	ctx := context.Background()

	produce, err := rootClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := rootClient.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}

	consume, err = nobodyClient.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "log_system/api/v1"
	"log_system/config/appconfig"
	"log_system/config/segmentconfig"
	"log_system/internal/log"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			fn(t, client, cfg)
			teardown()
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	listen, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	conn, err := grpc.Dial(listen.Addr().String(), clientOpts...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	commitLog, err := log.New(dir, &appconfig.AppConfig{
		Port:    0,
		Segment: &segmentconfig.SegmentCofig{},
	})
	require.NoError(t, err)

	cfg = &Config{CommitLog: commitLog}

	if fn != nil {
		fn(cfg)
	}

	srv, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go srv.Serve(listen)

	client = api.NewLogClient(conn)

	return client, cfg, func() {
		srv.Stop()
		conn.Close()
		listen.Close()
		commitLog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: want,
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, cfg *Config) {
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
		stream, err := client.ProduceStream(ctx)
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
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
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

func testConsumePastBoundary(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
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
}

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"distributed-systems/1_mapreduce/cmd/distributed_mr/logger"
	rpcArgs "distributed-systems/1_mapreduce/cmd/distributed_mr/rpc"
)

type Coordinator struct {
	inputFiles        []string
	intermediateFiles []string
	outputFiles       []string
	nReduce           int64

	log *slog.Logger
	mu  sync.Mutex
}

func (c *Coordinator) serve() {
	if err := rpc.Register(c); err != nil {
		c.log.Error("failed to register rpc server", slog.Any("error", err))
		os.Exit(1)
	}
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	socketName := rpcArgs.CoordinatorSock()
	c.log.Debug("coordinator socket", slog.String("socket", socketName))

	if err := os.Remove(socketName); err != nil {
		c.log.Error("failed to remove socket", slog.Any("error", err))
		os.Exit(1)
	}

	l, err := net.Listen("unix", socketName)
	if err != nil {
		c.log.Error("failed to listen socket", slog.Any("error", err))
		os.Exit(1)
	}

	if err = http.Serve(l, nil); err != nil {
		c.log.Error("failed to serve server", slog.Any("error", err))
		os.Exit(1)
	}
}

// The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks
// Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
func newCoordinator(log *slog.Logger, files []string, nReduce int64) *Coordinator {
	coordinator := &Coordinator{
		inputFiles:        files,
		intermediateFiles: make([]string, 0, len(files)),
		nReduce:           nReduce,
		log:               log,
	}

	coordinator.log.Debug("input files", slog.Int("length", len(files)))
	coordinator.log.Debug("coordinator",
		slog.Any("input files", coordinator.inputFiles),
		slog.Int64("reduce num", coordinator.nReduce),
	)

	go coordinator.serve()

	return coordinator
}

func (c *Coordinator) Handshake(req *rpcArgs.HandshakeRequest, res *rpcArgs.HandshakeResponse) error {
	c.log.Info("got handshake request", slog.String("worker", req.WorkerName))

	res.X = req.X + req.Y
	return nil
}

func (c *Coordinator) GiveTask(req *rpcArgs.GiveTaskRequest, res *rpcArgs.GiveTaskResponse) error {
	c.log.Info("got task request", slog.String("worker", req.WorkerName))

	// todo think about recovery files

	c.log.Debug("coordinator",
		slog.Int("input files length", len(c.inputFiles)),
		slog.Int("intermediate files length", len(c.intermediateFiles)),
	)

	if len(c.inputFiles) != 0 {
		res.WorkerName = req.WorkerName
		res.Map = true
		res.Reduce = false
		res.File = c.inputFiles[0]
		res.NReduce = c.nReduce

		c.inputFiles = c.inputFiles[1:]

		return nil
	}

	if len(c.intermediateFiles) != 0 {
		res.WorkerName = req.WorkerName
		res.Map = false
		res.Reduce = true
		res.File = c.intermediateFiles[0]
		res.NReduce = c.nReduce

		c.intermediateFiles = c.intermediateFiles[1:]

		return nil
	}

	return nil
}

func (c *Coordinator) process(ctx context.Context) {
	intermediateFilesTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			c.log.Info("coordinator stopped")
			return

		case <-intermediateFilesTicker.C:
			c.processIntermediateFiles()
		}
	}
}

func (c *Coordinator) processIntermediateFiles() {
	const intermediateDir = "intermediate"

	dir, err := os.ReadDir(intermediateDir)
	if err != nil {
		c.log.Error("failed to read dir", slog.String("dirname", intermediateDir), slog.Any("error", err))
		return
	}

	if len(dir) == 0 {
		c.log.Info("intermediate dir is empty")
		c.intermediateFiles = []string{}
		return
	}

	for _, file := range dir {
		fileName := fmt.Sprintf("%s/%s", intermediateDir, file.Name())

		if slices.Contains(c.intermediateFiles, fileName) {
			continue
		}

		c.intermediateFiles = append(c.intermediateFiles, fileName)
	}

	c.log.Debug("coordinator intermediate files",
		slog.Int("length", len(c.intermediateFiles)),
		slog.Any("files", c.intermediateFiles),
	)
}

func (c *Coordinator) Map(filename string) {

}

func (c *Coordinator) Reduce() {}

func main() {
	log := logger.New()

	log.Info("coordinator started")

	if len(os.Args) < 2 {
		log.Error("usage: coordinator <files...> <reduce num>", slog.Int("args length", len(os.Args)))
		os.Exit(1)
	}

	reduceNumber := os.Args[len(os.Args)-1]
	nReduce, err := strconv.ParseInt(reduceNumber, 10, 64)
	if err != nil {
		log.Error("usage: coordinator <files...> <reduce num>", slog.String("reduce num", reduceNumber))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Debug("waiting for signal")
		<-sig

		log.Info("shutting down")
		cancel()
	}()

	coordinator := newCoordinator(log, os.Args[1:len(os.Args)-1], nReduce)
	coordinator.process(ctx)
}

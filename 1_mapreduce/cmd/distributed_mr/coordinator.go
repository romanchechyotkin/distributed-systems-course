package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"

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
		inputFiles: files,
		nReduce:    nReduce,
		log:        log,
	}

	coordinator.log.Debug("input files", slog.Int("length", len(files)))
	coordinator.log.Debug("coordinator",
		slog.Any("input files", coordinator.inputFiles),
		slog.Int64("reduce num", coordinator.nReduce),
	)

	// todo

	return coordinator
}

func (c *Coordinator) Handshake(req *rpcArgs.HandshakeRequest, res *rpcArgs.HandshakeResponse) error {
	c.log.Info("got handshake request", slog.String("worker", req.WorkerName))

	res.X = req.X + req.Y
	return nil
}

func (c *Coordinator) GiveTask(req *rpcArgs.GiveTaskRequest, res *rpcArgs.GiveTaskResponse) error {
	c.log.Info("got task request", slog.String("worker", req.WorkerName))

	if len(c.inputFiles) > 0 {
		res.WorkerName = req.WorkerName
		res.Map = true
		res.Reduce = false
		res.File = c.inputFiles[0]
		res.NReduce = c.nReduce

		c.inputFiles = c.inputFiles[1:]
	}

	c.log.Debug("coordinator input files",
		slog.Int("length", len(c.inputFiles)),
		slog.Any("files", c.inputFiles),
	)

	return nil
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

	coordinator := newCoordinator(log, os.Args[1:len(os.Args)-1], nReduce)
	coordinator.serve()
}

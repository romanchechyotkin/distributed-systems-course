package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"

	rpcArgs "distributed-systems/1_mapreduce/cmd/distributed_mr/rpc"
)

type Coordinator struct {
	inputFiles        []string
	intermediateFiles []string
	outputFiles       []string
	nReduce           int64

	mu sync.Mutex
}

func (c *Coordinator) serve() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	socketName := rpcArgs.CoordinatorSock()
	log.Println("coordinator socket:", socketName)
	os.Remove(socketName)

	l, e := net.Listen("unix", socketName)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l, nil)
}

// The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks
// Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
func newCoordinator(files []string, nReduce int64) *Coordinator {
	coordinator := &Coordinator{
		inputFiles: files,
		nReduce:    nReduce,
	}

	// todo

	coordinator.serve()
	return coordinator
}

func (c *Coordinator) Handshake(req *rpcArgs.HandshakeRequest, res *rpcArgs.HandshakeResponse) error {
	log.Println("[coordinator]: handshake; worker:", req.WorkerName)

	res.X = req.X + req.Y
	return nil
}

func (c *Coordinator) GiveTask(req *rpcArgs.GiveTaskRequest, res *rpcArgs.GiveTaskResponse) error {
	log.Println("[coordinator]: give task; worker:", req.WorkerName)

	if len(c.inputFiles) > 0 {
		res.WorkerName = req.WorkerName
		res.Map = true
		res.Reduce = false
		res.Files = c.inputFiles
	}

	return nil
}

func (c *Coordinator) Map() {}

func (c *Coordinator) Reduce() {}

func main() {
	log.Println("coordinator started")

	if len(os.Args) < 2 {
		log.Println("usage: coordinator <files...> <reduce num>")
		os.Exit(1)
	}

	reduceNumber := os.Args[len(os.Args)-1]
	nReduce, err := strconv.ParseInt(reduceNumber, 10, 64)
	if err != nil {
		log.Fatal("last argument must be a number of reduce tasks")
	}

	_ = newCoordinator(os.Args[1:len(os.Args)-1], nReduce)
}

package main

import (
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"

	"distributed-systems/1_mapreduce/cmd/distributed_mr/logger"
	rpcArgs "distributed-systems/1_mapreduce/cmd/distributed_mr/rpc"
	"distributed-systems/1_mapreduce/cmd/distributed_mr/utils"
	"distributed-systems/1_mapreduce/types"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int64 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int64(h.Sum32() & 0x7fffffff)
}

// Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker.
// There will be just one coordinator process, and one or more worker processes executing in parallel.

// In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine.

// The workers will talk to the coordinator via RPC. Each worker process will, in a loop, ask the coordinator for a task,
// read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task.

// The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

type worker struct {
	log     *slog.Logger
	name    string
	mapf    func(string) []types.KeyValue
	reducef func(string, []string) string
}

func newWorker(log *slog.Logger, mapF func(content string) []types.KeyValue, reduceF func(key string, values []string) string) *worker {
	workerName := utils.WorkerName()
	log = log.With(slog.String("worker name", workerName))

	w := &worker{
		log:     log,
		name:    workerName,
		mapf:    mapF,
		reducef: reduceF,
	}

	w.log.Debug("worker")

	w.handshake()

	return w
}

func (w *worker) process() {
	for {
		log.Println("question")
	}
}

func (w *worker) processMap(nReduce int64, files ...string) {

}

func (w *worker) processReduce() {

}

func (w *worker) handshake() {
	w.log.Info("handshake request")

	args := rpcArgs.HandshakeRequest{}

	args.X = 1
	args.Y = 1
	args.WorkerName = w.name

	reply := rpcArgs.HandshakeResponse{}

	err := w.call("Coordinator.Handshake", &args, &reply)
	if err != nil {
		w.log.Error("Coordinator.Handshake failed", slog.Any("error", err))
		os.Exit(1)
	}

	if reply.X != args.X+args.Y {
		w.log.Error("Coordinator.Handshake failed", slog.Any("error", err))
		os.Exit(1)
	}

	w.log.Info("Coordinator.Handshake succeeded")
}

func (w *worker) requestCoordinator() (*rpcArgs.GiveTaskResponse, error) {
	args := rpcArgs.GiveTaskRequest{}

	args.WorkerName = w.name

	reply := rpcArgs.GiveTaskResponse{}

	err := w.call("Coordinator.GiveTask", &args, &reply)
	if err != nil {
		log.Println("Coordinator.GiveTask failed")
		return nil, err
	}

	log.Printf("Coordinator.GiveTask succeeded")
	return &reply, nil
}

// call send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w *worker) call(rpcName string, args any, reply any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	socketName := rpcArgs.CoordinatorSock()
	w.log.Debug("coordinator socket", slog.String("socket", socketName))

	c, err := rpc.DialHTTP("unix", socketName)
	if err != nil {
		w.log.Error("failed to dial rpc server rpc", slog.Any("error", err))
		return err
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err != nil {
		w.log.Error("failed to call rpc", slog.Any("error", err))
		return err
	}

	return nil
}

func main() {
	log := logger.New()

	if len(os.Args) != 2 {
		log.Error("usage: worker <wc plugin path>")
		os.Exit(1)
	}

	mapf, reducef := utils.LoadPlugin(os.Args[1])

	w := newWorker(log, mapf, reducef)
	w.log.Info("worker started")
	w.process()
}

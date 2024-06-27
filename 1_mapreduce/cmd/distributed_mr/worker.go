package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

func newWorker(log *slog.Logger) *worker {
	workerName := utils.WorkerName()
	log = log.With(slog.String("worker name", workerName))

	mapf, reducef := utils.LoadPlugin(os.Args[1])

	w := &worker{
		log:     log,
		name:    workerName,
		mapf:    mapf,
		reducef: reducef,
	}

	w.log.Debug("worker")

	w.handshake()

	return w
}

func (w *worker) process(ctx context.Context) {
	taskTicker := time.NewTicker(time.Second * 5)

	for {
		select {
		case <-ctx.Done():
			w.log.Info("process stopped")
			return

		case <-taskTicker.C:
			taskResponse, err := w.requestCoordinator()
			if err != nil {
				w.log.Error("failed to request coordinator", slog.Any("error", err))
				continue
			}

			w.log.Info("got task response",
				slog.Bool("map", taskResponse.Map),
				slog.Bool("reduce", taskResponse.Reduce),
				slog.String("filename", taskResponse.File),
			)

			if taskResponse.Map == false && taskResponse.Reduce == false {
				continue
			}

			if taskResponse.Map {
				w.log.Debug("MAP FUNC")
				if err = w.processMap(taskResponse.File); err != nil {
					w.log.Error("failed to map file content", slog.Any("error", err))
				}
			}

			if taskResponse.Reduce {
				w.log.Debug("REDUCE FUNC")
			}
		}
	}
}

func (w *worker) processMap(file string) error {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		w.log.Error("failed to read file", slog.Any("error", err))
		return err
	}

	w.log.Debug("file content", slog.Int("length", len(fileContent)))

	keyValues := w.mapf(string(fileContent))

	w.log.Debug("key values",
		slog.Any("example", keyValues[0]),
		slog.Int("length", len(keyValues)),
	)

	file = strings.Split(file, "/")[1]
	fileName := fmt.Sprintf("intermediate/%s", file)

	openFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		w.log.Error("failed to open file", slog.String("filename", fileName), slog.Any("error", err))
		return err
	}

	for _, kv := range keyValues {
		_, err = openFile.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
		if err != nil {
			w.log.Error("failed to write to file", slog.Any("error", err))
			continue
		}
	}

	return nil
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
		w.log.Error("Coordinator.GiveTask failed", slog.Any("error", err))
		return nil, err
	}

	w.log.Info("Coordinator.GiveTask succeeded")

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

	w := newWorker(log)
	w.log.Info("worker started")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		log.Debug("waiting for signal")
		<-sig

		if err := cleanup(); err != nil {
			log.Error("failed to cleanup dir", slog.String("dir", "intermediate"), slog.Any("error", err))
			return
		}

		log.Info("shutting down")
		cancel()
	}()

	w.process(ctx)
}

func cleanup() error {
	dir, err := os.ReadDir("intermediate")
	if err != nil {
		return err
	}

	for _, file := range dir {
		fileName := fmt.Sprintf("intermediate/%s", file.Name())
		err = os.Remove(fileName)
		if err != nil {
			continue
		}
	}

	return nil
}

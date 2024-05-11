package main

import (
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"plugin"
	"sync"
	"time"

	rpcArgs "distributed-systems/1_mapreduce/cmd/distributed_mr/rpc"
	"distributed-systems/1_mapreduce/types"
)

// Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker.
// There will be just one coordinator process, and one or more worker processes executing in parallel.

// In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine.

// The workers will talk to the coordinator via RPC. Each worker process will, in a loop, ask the coordinator for a task,
// read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task.

// The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

type worker struct {
	name    string
	mapf    func(string) []types.KeyValue
	reducef func(string, []string) string

	wg sync.WaitGroup
}

func randomWorkerName() string {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	letter := "abcdefghijklmnopqrstuvwxyz"
	var idx int
	var res string

	for len(res) != 10 {
		idx = rand.Intn(26)
		if idx == 0 {
			continue
		}

		res += string(letter[idx])
	}

	return res
}

func newWorker(mapF func(content string) []types.KeyValue, reduceF func(key string, values []string) string) *worker {
	w := &worker{
		name:    randomWorkerName(),
		mapf:    mapF,
		reducef: reduceF,
	}

	w.handshake()

	w.wg.Add(1)
	go w.process()

	return w
}

func (w *worker) process() {
	defer w.wg.Done()

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			res, err := w.requestCoordinator()
			if err != nil {
				log.Println(err)
			}

			if res.Map {
				w.processMap(res.Files...)
			}

			if res.Reduce {
				w.processReduce()
			}

			log.Println(res)

		}
	}
}

func (w *worker) processMap(files ...string) {
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return
		}

		kva := w.mapf(string(content))
		log.Println(kva)
	}

}

func (w *worker) processReduce() {

}

func (w *worker) handshake() {
	args := rpcArgs.HandshakeRequest{}

	// fill in the argument(s).
	args.X = 1
	args.Y = 1
	args.WorkerName = w.name

	// declare a reply structure.
	reply := rpcArgs.HandshakeResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Handshake", &args, &reply)
	if err != nil {
		log.Println("Coordinator.Handshake failed")
		os.Exit(1)
	}

	if reply.X != args.X+args.Y {
		log.Println("Coordinator.Handshake failed")
		os.Exit(1)
	}

	log.Printf("Coordinator.Handshake succeeded")
}

func (w *worker) requestCoordinator() (*rpcArgs.GiveTaskResponse, error) {
	args := rpcArgs.GiveTaskRequest{}

	// fill in the argument(s).
	args.WorkerName = w.name

	// declare a reply structure.
	reply := rpcArgs.GiveTaskResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.GiveTask", &args, &reply)
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
func call(rpcName string, args any, reply any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	socketName := rpcArgs.CoordinatorSock()
	log.Printf("coordinator socket: %s\n", socketName)

	c, err := rpc.DialHTTP("unix", socketName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Println("usage: worker <wc plugin path>")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	w := newWorker(mapf, reducef)
	log.Printf("worker %s started\n", w.name)

	w.wg.Wait()
}

func loadPlugin(path string) (func(content string) []types.KeyValue, func(key string, values []string) string) {
	plugin, err := plugin.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	mapF, err := plugin.Lookup("Map")
	if err != nil {
		log.Fatal(err)
	}

	mapFunc, ok := mapF.(func(content string) []types.KeyValue)
	if !ok {
		log.Fatal("no func map")
	}

	reduceF, err := plugin.Lookup("Reduce")
	if err != nil {
		log.Fatal(err)
	}

	reduceFunc, ok := reduceF.(func(key string, values []string) string)
	if !ok {
		log.Fatal("no func reduce")
	}

	return mapFunc, reduceFunc
}

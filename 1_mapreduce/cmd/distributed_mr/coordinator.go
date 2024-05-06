package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"distributed-systems/1_mapreduce/cmd/distributed_mr/logger"
	socket "distributed-systems/1_mapreduce/cmd/distributed_mr/rpc"
)

type Coordinator struct{}

func (c *Coordinator) serve() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := socket.CoordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l, nil)
}

func newCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := &Coordinator{}

	// todo

	coordinator.serve()
	return coordinator
}

type Request struct{}

type Response struct{}

func (c *Coordinator) Log(req *Request, res *Response) error {
	return nil
}

func main() {
	log := logger.New()
	log.Info("coordinator started")

	_ = newCoordinator(nil, 0)
}

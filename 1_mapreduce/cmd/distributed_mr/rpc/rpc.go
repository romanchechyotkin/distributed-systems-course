package rpc

import (
	"os"
	"strconv"
)

func CoordinatorSock() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type HandshakeRequest struct {
	WorkerName string
	X          int
	Y          int
}

type HandshakeResponse struct {
	X int
}

type GiveTaskRequest struct {
	WorkerName string
}

type GiveTaskResponse struct {
	//WorkerName string
	//Map        bool
	//Reduce     bool
}

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

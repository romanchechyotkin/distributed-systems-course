package main

import "distributed-systems/1_mapreduce/cmd/distributed_mr/logger"

type worker struct {
}

func main() {
	log := logger.New()
	log.Info("worker started")
}

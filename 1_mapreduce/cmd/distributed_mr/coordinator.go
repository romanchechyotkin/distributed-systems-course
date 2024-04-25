package main

import "distributed-systems/1_mapreduce/cmd/distributed_mr/logger"

type coordinator struct {
}

func main() {
	log := logger.New()
	log.Info("coordinator started")
}

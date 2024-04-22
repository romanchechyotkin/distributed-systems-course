package main

import (
	"context"
	"fmt"

	"log_system/config/appconfig"
)

func main() {
	cfg, err := appconfig.LoadFromPath(context.Background(), "pkl/dev/config.pkl")
	if err != nil {
		panic(err)
	}

	fmt.Printf("cfg %+v\n", cfg)
	fmt.Printf("cfg.Segment %+v\n", cfg.Segment)
}

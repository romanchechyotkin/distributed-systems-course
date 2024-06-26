package utils

import (
	"log"
	"plugin"

	"distributed-systems/1_mapreduce/types"
)

func LoadPlugin(path string) (func(content string) []types.KeyValue, func(key string, values []string) string) {
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

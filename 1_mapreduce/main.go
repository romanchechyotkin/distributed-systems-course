package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"plugin"
	"sort"
	"sync"

	"distributed-systems/1_mapreduce/types"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go xxx.so input files...")
		os.Exit(1)
	}

	mapf, reducef := loadMapReducePlugin(os.Args[1])

	var wg sync.WaitGroup
	var mu sync.Mutex

	intermediate := []types.KeyValue{}
	for _, filePath := range os.Args[2:] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatal(err)
			}

			keyValues := mapf(string(content))

			mu.Lock()
			intermediate = append(intermediate, keyValues...)
			mu.Unlock()
		}()
	}

	wg.Wait()

	log.Println("total length", len(intermediate))
	sort.Sort(types.ByKey(intermediate))

	oname := "wc"
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func loadMapReducePlugin(path string) (func(content string) []types.KeyValue, func(key string, values []string) string) {
	pluginFile, err := plugin.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	xmapF, err := pluginFile.Lookup("Map")
	if err != nil {
		log.Fatal(err)
	}
	mapF := xmapF.(func(content string) []types.KeyValue)

	xreduceF, err := pluginFile.Lookup("Reduce")
	if err != nil {
		log.Fatal(err)
	}
	reduceF := xreduceF.(func(string, []string) string)

	return mapF, reduceF
}

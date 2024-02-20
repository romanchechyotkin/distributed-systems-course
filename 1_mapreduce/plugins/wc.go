package main

import (
	"strconv"
	"strings"
	"unicode"

	"distributed-systems/1_mapreduce/types"
)

//
// Sequential MapReduce implementation for counting words in text
//

// Map function takes file content and returns array of KeyValue {"word": "1"}
func Map(content string) []types.KeyValue {
	ff := func(r rune) bool {
		return !unicode.IsLetter(r)
	}

	words := strings.FieldsFunc(content, ff)

	kv := make([]types.KeyValue, 0, len(words))
	for _, word := range words {
		kv = append(kv, types.KeyValue{
			Key:   word,
			Value: "1",
		})
	}

	return kv
}

// Reduce function returns amount of key occurrences
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

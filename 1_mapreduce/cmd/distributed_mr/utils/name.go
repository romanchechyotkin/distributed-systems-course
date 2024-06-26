package utils

import (
	"math/rand"
	"time"
)

func WorkerName() string {
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

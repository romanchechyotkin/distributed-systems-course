package main

import (
	"math/rand"
	"time"
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	count := 0
	ch := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			v := requestVote()
			ch <- v
		}()
	}

	for i := 0; i < 10; i++ {
		v := <-ch
		if v {
			count += 1
		}
	}

	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}

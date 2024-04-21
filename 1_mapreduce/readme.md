# Lab 1 - MapReduce

full description: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

tasks:
- implement Map and Reduce functions
- implement the workers
- implement the coordinator
- use RPC

run:
```shell
    go build -buildmode=plugin plugins/wc.go
    go run main.go wc.so texts/*.txt
```
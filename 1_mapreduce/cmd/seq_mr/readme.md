
run:
```shell
    go build -buildmode=plugin plugins/wc.go
    go run main.go wc.so texts/*.txt
```
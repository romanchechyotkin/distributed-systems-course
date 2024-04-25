CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: proto
proto:
	protoc api/v1/*.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=.

.PHONY: test
test:
	go test -race ./...

.PHONY: cert
cert:
	cfssl gencert -initca cert/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=cert/ca-config.json \
	-profile=server \
	cert/server-csr.json | cfssljson -bare server

	mv *.pem *.csr ${CONFIG_PATH}
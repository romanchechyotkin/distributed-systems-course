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

	cfssl gencert \
	-ca=ca.pem \
	-ca-key=ca-key.pem \
	-config=cert/ca-config.json \
	-profile=client \
	-cn="root" \
	cert/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=cert/ca-config.json \
    -profile=client \
	-cn="nobody" \
    cert/client-csr.json | cfssljson -bare nobody-client

	cp cert/acl/model.conf ${CONFIG_PATH}/model.conf
	cp cert/acl/policy.csv ${CONFIG_PATH}/policy.csv

	mv *.pem *.csr ${CONFIG_PATH}

	
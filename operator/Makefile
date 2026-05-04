IMG ?= ghcr.io/bwalia/nebuladb-operator:dev
KUBECTL ?= kubectl
CONTROLLER_GEN ?= controller-gen

.PHONY: all generate manifests fmt vet build test docker-build docker-push install uninstall deploy undeploy

all: build

generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

manifests:
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook \
		paths="./..." \
		output:crd:artifacts:config=config/crd/bases \
		output:rbac:artifacts:config=config/rbac \
		output:webhook:artifacts:config=config/webhook

fmt:
	go fmt ./...

vet:
	go vet ./...

build: generate manifests fmt vet
	go build -o bin/manager ./cmd/manager

test:
	go test ./... -coverprofile cover.out

docker-build:
	docker build -t $(IMG) .

docker-push:
	docker push $(IMG)

install: manifests
	$(KUBECTL) apply -f config/crd/bases

uninstall:
	$(KUBECTL) delete -f config/crd/bases

deploy: manifests
	$(KUBECTL) apply -k config/default

undeploy:
	$(KUBECTL) delete -k config/default

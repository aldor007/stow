.PHONY: test
WORKSPACE = $(shell pwd)

topdir = /tmp/$(pkg)-$(version)

all: container runcontainer
	@true

container:
	docker build --no-cache -t builder-stow test/

runcontainer:
	docker run -v $(WORKSPACE):/mnt/src/github.com/aldor007/stow builder-stow

test: clean vet
	go test -v ./...

vet:
	go vet ./...

clean:
	@true

VERSION := $(shell git describe --tags --always)

.PHONY: all binary release source test vendor clean

all: test binary

binary: target/vulcan

# release is created from the src tarball to ensure we have all dependencies included
# in the tarball. We don't want to rely on glide or 3rd party repos to produce
# a build since 3rd party repos can go away or be renamed.
release: target/vulcan-$(VERSION).linux-amd64.tar.gz

source: target/vulcan-$(VERSION)-src.tar.gz

test: vendor
	@echo ">> running tests"
	@go test $(shell glide novendor)

vendor: vendor/.touch

clean:
	@echo ">> removing target directory"
	@rm -fr target
	@echo ">> removing vendor directory"
	@rm -fr vendor

target/vulcan:
	@echo ">> compiling target/vulcan"
	@mkdir -p target
	@go build \
		-ldflags "-X main.version=$(VERSION)" \
		-o target/vulcan main.go

target/vulcan-$(VERSION)-src.tar.gz: vendor
	@echo ">> creating source tarball $@"
	@mkdir -p target
	@tar \
		--exclude='./target' \
		--exclude='.git' \
		-s '/^\./vulcan-$(VERSION)-src/' \
		-czf $@ .

target/vulcan-$(VERSION).linux-amd64.tar.gz: target/vulcan-$(VERSION)-src.tar.gz
	@echo ">> creating binary release tarball $@"

	@rm -fr target/go
	@mkdir -p target/go/src/github.com/digitalocean/vulcan
	
	@tar \
		-xf target/vulcan-$(VERSION)-src.tar.gz \
		--strip-components=1 \
		-C target/go/src/github.com/digitalocean/vulcan
	
	@rm -fr target/release
	@mkdir -p target/release/bin
	
	@GOPATH=$(shell pwd)/target/go \
	GOARCH="amd64" \
	GOOS="linux" \
	go build \
		-ldflags "-X main.version=$(VERSION)" \
		-o target/release/bin/vulcan \
		target/go/src/github.com/digitalocean/vulcan/main.go
	
	@cp LICENSE README.md target/release/
	
	@tar \
		-s '/target.release/vulcan-$(VERSION).linux-amd64/' \
		-czf $@ target/release

	@rm -fr target/release
	@rm -fr target/go

glide.lock: glide.yaml
	@echo ">> updating glide.lock"
	@glide up

vendor/.touch: glide.lock glide.yaml
	@echo ">> installing golang dependencies into vendor directory"
	@glide install
	@echo ">> removing dependencies' committed vendor directories"
	@rm -fr vendor/github.com/prometheus/prometheus/vendor
	@touch vendor/.touch

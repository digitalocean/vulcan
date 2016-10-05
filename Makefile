GIT_SUMMARY := $(shell git describe --tags --dirty --always)
GO_VERSION := $(shell go version)

VULCAN_SRCS := $(shell find . -type f -iname '*.go' -not -path './vendor/*')
# lazy eval for VENDOR_SRCS since make vendor can change this value
VENDOR_SRCS = $(shell find vendor -type f -iname '*.go')
# lazy eval for SRC_FULL for any file that should be included in the src-full tarball
SRC_FULL = $(shell find . -type f -not -path './target/*' -not -path './.*')

.PHONY: all bin clean docker licensecheck lint push tar test vendor vet

###
# phony targets

all: tar

bin: target/vulcan_linux_amd64

clean:
	@echo ">> removing target directory"
	@rm -fr target
	@echo ">> removing vendor directory"
	@rm -fr vendor
	@echo ">> removing makecache directory"
	@rm -fr .makecache

docker: .makecache/docker

licensecheck:
	@echo ">> checking for license"
	@scripts/licensecheck

lint:
	@echo ">> linting source"
	@glide nv | xargs -L 1 golint

push: .makecache/docker
	@echo ">> pushing docker image dovulcan/vulcan:$(GIT_SUMMARY)"
	@docker push dovulcan/vulcan:$(GIT_SUMMARY)

tar: target/vulcan-$(GIT_SUMMARY).linux-amd64.tar.gz target/vulcan-$(GIT_SUMMARY).src-full.tar.gz

test: vendor
	@echo ">> running tests"
	@go test $$(glide nv)

vendor: .makecache/vendor

vet:
	@echo ">> vetting source"
	@go vet $(shell glide novendor)

###
# real targets

glide.lock: glide.yaml
	@echo ">> updating glide.lock"
	@glide up

.makecache:
	@echo ">> making directory $@"
	@mkdir $@

.makecache/docker: .makecache Dockerfile target/vulcan_linux_amd64
	@echo ">> building docker image dovulcan/vulcan:$(GIT_SUMMARY)"
	@docker build -t dovulcan/vulcan:$(GIT_SUMMARY) .
	@touch $@

.makecache/test: .makecache/vendor $(VULCAN_SRCS)
	@echo ">> running tests"
	@go test $$(glide nv)
	@touch $@

.makecache/vendor: .makecache glide.lock glide.yaml $(VENDOR_SRCS)
	@echo ">> installing golang dependencies into vendor directory"
	@glide install
	@echo ">> removing dependencies' committed vendor directories"
	@rm -fr vendor/github.com/prometheus/prometheus/vendor
	@touch $@

target/vulcan_linux_amd64: .makecache/vendor $(VULCAN_SRCS)
	@echo ">> building $@"
	@GOOS="linux" GOARCH="amd64" go build -ldflags="-X 'main.gitSummary=$(GIT_SUMMARY)' -X 'main.goVersion=$(GO_VERSION)'" -o $@ main.go

target/vulcan_darwin_amd64: .makecache/vendor $(VULCAN_SRCS)
	@echo ">> building $@"
	@GOOS="darwin" GOARCH="amd64" go build -ldflags="-X 'main.gitSummary=$(GIT_SUMMARY)' -X 'main.goVersion=$(GO_VERSION)'" -o $@ main.go

target/vulcan-$(GIT_SUMMARY).linux-amd64.tar.gz: target/vulcan_linux_amd64 LICENSE README.md
	@echo ">> creating tarball $@"
	@rm -fr target/tmp
	@mkdir -p target/tmp/vulcan-$(GIT_SUMMARY).linux-amd64/bin
	@cp target/vulcan_linux_amd64 target/tmp/vulcan-$(GIT_SUMMARY).linux-amd64/bin/vulcan
	@cp LICENSE README.md target/tmp/vulcan-$(GIT_SUMMARY).linux-amd64/
	@tar czf $@ -C target/tmp/ .
	@rm -fr target/tmp

target/vulcan-$(GIT_SUMMARY).src-full.tar.gz: $(SRC_FULL)
	@echo ">> creating tarball $@"
	@tar -zc --exclude='./target/' --exclude='./.*' -s '/^\./vulcan-$(GIT_SUMMARY).src-full/' -f $@ .

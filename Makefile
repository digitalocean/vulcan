GIT_SUMMARY := $(shell git describe --tags --dirty --always)
GO_VERSION := $(shell go version)
SRCS := $(shell find . -type f -iname '*.go')

.PHONY: all bin clean docker licensecheck lint push release tarballs test vendor vet

###
# phony targets

all: release

bin: target/vulcan_linux_amd64

clean:
	@echo ">> removing target directory"
	@rm -fr target
	@echo ">> removing vendor directory"
	@rm -fr vendor

docker: target/.dockertouch

licensecheck:
	@echo ">> checking for license"
	@scripts/licensecheck

lint:
	@echo ">> linting source"
	@glide nv | xargs -L 1 golint

push: target/.dockertouch
	@echo ">> pushing docker image dovulcan/vulcan:$(GIT_SUMMARY)"
	@docker push dovulcan/vulcan:$(GIT_SUMMARY)

release: target/.testtouch licensecheck lint vet tarballs

tarballs: target/vulcan-$(GIT_SUMMARY).linux-amd64.tar.gz target/vulcan-$(GIT_SUMMARY).src-full.tar.gz

test:
	@echo ">> running tests"
	@go test $$(glide nv)

vendor: vendor/.touch

vet:
	@echo ">> vetting source"
	@go vet $(shell glide novendor)

###
# real targets

glide.lock: glide.yaml
	@echo ">> updating glide.lock"
	@glide up

target/.dockertouch: Dockerfile target/vulcan_linux_amd64
	@echo ">> building docker image dovulcan/vulcan:$(GIT_SUMMARY)"
	@docker build -t dovulcan/vulcan:$(GIT_SUMMARY) .
	@touch target/.dockertouch

target/.testtouch: vendor/.touch $(SRCS)
	@echo ">> running tests"
	@go test $$(glide nv)
	@touch target/.testtouch

target/vulcan_linux_amd64: vendor/.touch $(SRCS)
	@echo ">> building $@"
	@GOOS="linux" GOARCH="amd64" go build -ldflags="-X 'main.gitSummary=$(GIT_SUMMARY)' -X 'main.goVersion=$(GO_VERSION)'" -o $@ main.go

target/vulcan_darwin_amd64: vendor/.touch $(SRCS)
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

target/vulcan-$(GIT_SUMMARY).src-full.tar.gz: $(shell find . -type f -not -path './target/*' -not -path './.git/*')
	@echo ">> creating tarball $@"
	@tar -zc --exclude='./target/' --exclude='./.git/' -s '/^\./vulcan-$(GIT_SUMMARY).src-full/' -f $@ .

vendor/.touch: glide.lock glide.yaml
	@echo ">> installing golang dependencies into vendor directory"
	@glide install
	@echo ">> removing dependencies' committed vendor directories"
	@rm -fr vendor/github.com/prometheus/prometheus/vendor
	@touch vendor/.touch

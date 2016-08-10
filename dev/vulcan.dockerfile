FROM golang:1.6.3-wheezy

ENV GLIDEPATH /glide
ENV GLIDE_DOWNLOAD_URL https://github.com/Masterminds/glide/releases/download/v0.11.1/glide-v0.11.1-linux-amd64.tar.gz
ENV GLIDE_DOWNLOAD_SHA256 de0c7870738c6bc11128761d53a99ad68687b0a213fe52cea15ad05d93f10e42

RUN mkdir -p $GLIDEPATH \
 && curl -fsSL $GLIDE_DOWNLOAD_URL -o glide.tar.gz \
 && echo "$GLIDE_DOWNLOAD_SHA256  glide.tar.gz" | sha256sum -c - \
 && tar -xf glide.tar.gz --strip-components=1 -C /usr/local/bin linux-amd64/glide \
 && rm glide.tar.gz

# Only copy in glide.yaml and glide.lock so that no changes are triggered and
# we can use the docker cache. We also need to remove our dependencies' vendor
# dirs to avoid build errors.
COPY glide.yaml glide.lock $GLIDEPATH/
WORKDIR $GLIDEPATH
RUN glide install \
 && rm -fr $GLIDEPATH/vendor/github.com/prometheus/prometheus/vendor

RUN mkdir -p $GOPATH/src/github.com/digitalocean/vulcan
# .dockerignore excludes developer's local vendor directory  
COPY . $GOPATH/src/github.com/digitalocean/vulcan/
WORKDIR $GOPATH/src/github.com/digitalocean/vulcan
RUN ln -s $GLIDEPATH/vendor $GOPATH/src/github.com/digitalocean/vulcan/vendor \
 && go build -o $GOPATH/bin/vulcan main.go

ENTRYPOINT ["vulcan"]

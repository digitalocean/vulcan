FROM golang:1.7.3-wheezy

ENV GLIDEPATH /glide
ENV GLIDE_DOWNLOAD_URL https://github.com/Masterminds/glide/releases/download/v0.12.2/glide-v0.12.2-linux-amd64.tar.gz
ENV GLIDE_DOWNLOAD_SHA256 edd398b4e94116b289b9494d1c13ec2ea37386bad4ada91ecc9825f96b12143c

RUN mkdir -p $GLIDEPATH \
 && curl -fsSL $GLIDE_DOWNLOAD_URL -o glide.tar.gz \
 && echo "$GLIDE_DOWNLOAD_SHA256  glide.tar.gz" | sha256sum -c - \
 && tar -xf glide.tar.gz --strip-components=1 -C /usr/local/bin linux-amd64/glide \
 && rm glide.tar.gz

#  && apt-get update \
#  && apt-get install build-essential

RUN mkdir -p /go/src/github.com/digitalocean \
 && ln -s /vulcan /go/src/github.com/digitalocean/
WORKDIR /go/src/github.com/digitalocean/vulcan

ENTRYPOINT ["scripts/build"]

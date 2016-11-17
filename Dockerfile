FROM debian:8.6
 
COPY target/vulcan_linux_amd64 /usr/local/bin/vulcan
ENTRYPOINT ["vulcan"]

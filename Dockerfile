FROM busybox:glibc
 
COPY target/vulcan_linux_amd64 /usr/local/bin/vulcan
ENTRYPOINT ["vulcan"]

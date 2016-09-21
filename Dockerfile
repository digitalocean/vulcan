FROM busybox:glibc
 
COPY target/linux/vulcan /usr/local/bin/vulcan
ENTRYPOINT ["vulcan"]

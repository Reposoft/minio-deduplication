#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

curl -f --retry 3 --retry-connrefused http://app0:2112/metrics > /dev/null

retrywait=0
until mc --no-color config host add minio0 http://minio0:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; \
  do [ $(( retrywait++ )) -lt 5 ]; sleep 1; done

mc --no-color mb minio0/bucket.write

echo "Added before watch" > test1.txt
mc --no-color cp --attr "Content-Type=text/testing1" test1.txt minio0/bucket.write/
mc --no-color stat --json minio0/bucket.write/test1.txt | grep '"Content-Type":"text/testing1"'

mc --no-color mb minio0/bucket.read

expected=minio0/bucket.read/$(sha256sum test1.txt | cut -d' ' -f1).txt
retrywait=0
until mc --no-color stat "$expected"; \
  do [ $(( retrywait++ )) -lt 10 ]; sleep 1; done
mc --no-color stat --json "$expected" | grep '"Content-Type":"text/testing1"'

mc --no-color ls minio0/bucket.write
mc --no-color ls minio0/bucket.read

echo "Added after watch start" > test2.txt
mc --no-color cp --attr "Content-Type=text/testing2" test2.txt minio0/bucket.write/

expected=minio0/bucket.read/$(sha256sum test2.txt | cut -d' ' -f1).txt
retrywait=0
until mc --no-color stat "$expected"; \
  do [ $(( retrywait++ )) -lt 10 ]; sleep 1; done

# TODO dir sharding option 0-3

# TODO X-Custom-Header

echo "Test completed"

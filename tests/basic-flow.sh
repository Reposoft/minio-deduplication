#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

retrywait=0
until mc --no-color config host add minio0 http://minio0:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY \
     || [ $retrywait -eq 9 ]; do sleep $(( retrywait++ )); done

mc --no-color mb minio0/bucket.write
mc --no-color mb minio0/bucket.read

echo "Test1" > test1.txt
mc --no-color cp test1.txt minio0/bucket.write/

expected=minio0/bucket.read/$(sha256sum test1.txt | cut -d' ' -f1).txt

retrywait=0
until mc --no-color stat "$expected" \
     || [ $retrywait -eq 5 ]; do sleep $(( retrywait++ )); done

# TODO Content-Disposition

# TODO dir sharding option 0-3

# TODO X-Custom-Header

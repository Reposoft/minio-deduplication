#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="Some file.JPG"
echo "BLOB" > "$name"

curl -f -v --retry 3 -T "$name" "http://minio0:9000/bucket.write/$(echo -n $name | sed 's/ /%20/g' )"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
sleep 1
echo "# Extensions should be lower case"
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.jpg | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition: attachment; filename="Some file.JPG"'

name="Some file.JPEG"
echo "BLOB2" > "$name"

curl -f -v --retry 3 -T "$name" "http://minio0:9000/bucket.write/$(echo -n $name | sed 's/ /%20/g' )"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
sleep 1
echo "# The extension JPEG is a special case that we change to jpg"
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.jpg | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition: attachment; filename="Some file.JPEG"'


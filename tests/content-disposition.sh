#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

mc --no-color policy set upload minio0/bucket.write

mc --no-color policy set download minio0/bucket.read

name="My blob (1).txt"
echo "My blob with a name" > "$name"

curl -f -v --retry 3 -T "$name" "http://minio0:9000/bucket.write/$(echo -n $name | sed 's/ /%20/g' )"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition: attachment; filename="My blob (1).txt"'

#!/bin/bash
set -eEo pipefail

[ "$TESTS_DISABLED" = "true" ] && echo "Tests disabled through env TESTS_DISABLED=true" && exit 0

function onerr {
  CALLER="$(caller)"
  LINE=$1
  echo "^^^^^ ERR $CALLER line $LINE ^^^^^"
  after-all.sh || true
  echo "_____ ERR $CALLER line $LINE _____"
}

trap 'onerr $LINENO' ERR

# If we tweak kafka client config we can probably be faster
[ -n "$ACCEPTABLE_TRANSFER_DELAY" ] || export ACCEPTABLE_TRANSFER_DELAY=1
echo "Note: Tests rely on an arbitrary delay, ${ACCEPTABLE_TRANSFER_DELAY}s"
echo "If existence checks fail, try increasing with ACCEPTABLE_TRANSFER_DELAY="

sleep 1

# basic-flow also does bucket setup for the other tests
basic-flow.sh
# content-disposition sets bucket policies
content-disposition.sh
# # remaining tests should not depend on each other
custom-metadata.sh
deduplication.sh
extensions.sh
folders.sh
tagging.sh
zero-padded-checksums.sh
filename-encoding.sh
upload-path-tracking.sh

echo "_____ all tests executed _____"
after-all.sh
echo "_____         ok         _____"

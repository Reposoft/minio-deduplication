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

echo "_____ batch test start _____"
sleep 1

basic-flow.sh

echo "_____ batch test executed _____"
after-all.sh
echo "_____         ok         _____"

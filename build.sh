#!/usr/bin/env bash
[ -z "$DEBUG" ] || set -x
set -eo pipefail

[ -n "$PLATFORMS" ] || PLATFORMS="linux/amd64,linux/arm64/v8"
[ -n "$PLATFORM" ]  || PLATFORM="--platform=$PLATFORMS"
[ -z "$REGISTRY" ]  || PREFIX="$REGISTRY/"
[ -n "$NOPUSH" ]    || BUILDX_PUSH="--push"
[ -n "$IMAGE" ]     || IMAGE="solsson/minio-deduplication"

SOURCE_COMMIT=$(git rev-parse --verify HEAD 2>/dev/null || echo '')
if [[ ! -z "$SOURCE_COMMIT" ]]; then
  GIT_STATUS=$(git status --untracked-files=normal --porcelain=v2 | grep -v ' hooks/build' || true)
  if [[ ! -z "$GIT_STATUS" ]]; then
    SOURCE_COMMIT="$SOURCE_COMMIT-dirty"
  fi
fi

[ -n "$NOTEST" ] || ./test.sh

docker buildx build --pull $BUILDX_PUSH --progress=plain $PLATFORM -t ${PREFIX}${IMAGE}:$SOURCE_COMMIT$XTAG .

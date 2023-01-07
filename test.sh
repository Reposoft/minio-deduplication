#!/bin/bash

echo "DOCKER_DEFAULT_PLATFORM=$DOCKER_DEFAULT_PLATFORM"

#  Note that to troubleshoot it's better to run docker-compose -f docker-compose.test.yml up

for TEST in \
    "-f docker-compose.test.yml" \
    "-f docker-compose.test.yml -f docker-compose.test-kafka.yml" \
    ; do
  echo "=> $TEST"
  TESTS_DISABLED=false docker-compose $TEST up --build --abort-on-container-exit --exit-code-from sut sut
  RESULT=$?
  echo "=> Logs from the other containers:"
  docker-compose $TEST logs --no-color | grep -v 'sut-1[[:space:]]*|'
  echo "=> Exit $RESULT for $TEST"
  docker-compose $TEST down --remove-orphans -v
  [ $RESULT -eq 0 ] || exit $RESULT 
done 

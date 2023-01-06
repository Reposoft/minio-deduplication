FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2022-12-24T15-21-38Z

RUN microdnf install jq --nodocs

COPY *.sh /usr/local/bin/

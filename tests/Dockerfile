FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2022-01-07T06-01-38Z

RUN microdnf install jq --nodocs

COPY *.sh /usr/local/bin/

FROM --platform=${TARGETPLATFORM:-linux/amd64} vectorized/redpanda:v22.3.10@sha256:dc1f47dcc592857ea7831e2a28829ebca95a9cd350d6e6374a98414866b62c6f

FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2022-12-24T15-21-38Z

RUN microdnf install jq --nodocs

COPY --from=0 /usr/bin/rpk /usr/bin/rpk
COPY --from=0 /opt/redpanda/libexec/rpk /opt/redpanda/libexec/rpk

COPY *.sh /usr/local/bin/

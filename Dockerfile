FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.23.8-bookworm

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

RUN go test ./...

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
  CGO_ENABLED=0 \
  go build -ldflags '-w -extldflags "-static"'

FROM --platform=${TARGETPLATFORM:-linux/amd64} gcr.io/distroless/static-debian12:nonroot

COPY --from=0 /workspace/source/minio-deduplication /usr/local/bin/minio-deduplication

ENTRYPOINT ["minio-deduplication"]

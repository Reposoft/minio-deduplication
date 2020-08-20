FROM golang:1.14.7-alpine3.12@sha256:e9f6373299678506eaa6e632d5a8d7978209c430aa96c785e5edcb1eebf4885e

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

# See test.sh
#RUN go test

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags '-w -extldflags "-static"'

FROM gcr.io/distroless/base:nonroot@sha256:e995d1bbbf277050ddc2ca9df0fe01c6fd820c2bfa6d6edb3a0452614a939912

COPY --from=0 /workspace/source/v1 /usr/local/bin/v1

ENTRYPOINT ["v1"]

FROM golang:1.14.2-alpine3.11@sha256:9b3ad7928626126b72b916609ad081cfb6c0149f6e60cef7fc1e9e15a0d1e973

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

# See test.sh
#RUN go test

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags '-w -extldflags "-static"'

FROM gcr.io/distroless/base:nonroot@sha256:54c459100e9d420e023b0aecc43f7010d2731b6163dd8e060906e2dec4c59890

COPY --from=0 /workspace/source/v1 /usr/local/bin/v1

ENTRYPOINT ["v1"]

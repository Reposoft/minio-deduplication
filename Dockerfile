FROM golang:1.17.3-bullseye@sha256:01ade9881e2f3c74662c1915a8b77c2019d4ce0b29b9ea792997de2cdd237cb6

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

# See test.sh
#RUN go test

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags '-w -extldflags "-static"'

FROM gcr.io/distroless/base:nonroot@sha256:46d4514c17aca7a68559ee03975983339fc548e6d1014e2d7633f9123f2d3c59

COPY --from=0 /workspace/source/v1 /usr/local/bin/v1

ENTRYPOINT ["v1"]

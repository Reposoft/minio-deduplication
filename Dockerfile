FROM golang:1.13.9-alpine3.11@sha256:7d45a6fc9cde63c3bf41651736996fe94a8347e726fe581926fd8c26e244e3b2

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

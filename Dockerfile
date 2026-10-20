FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/minibroker ./cmd/minibroker \
 && CGO_ENABLED=0 go build -o /out/mbctl ./cmd/mbctl

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
EXPOSE 50051
ENTRYPOINT ["minibroker"]

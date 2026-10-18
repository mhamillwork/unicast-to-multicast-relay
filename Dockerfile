# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/mcast-relay ./cmd/mcast-relay \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -o /out/pkt ./cmd/pkt

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mcast-relay /mcast-relay
COPY --from=build /out/pkt /pkt
USER nonroot:nonroot
EXPOSE 9100/tcp
ENTRYPOINT ["/mcast-relay"]
CMD ["--config", "/etc/mcast-relay/config.yaml"]

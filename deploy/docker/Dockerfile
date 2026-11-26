# syntax=docker/dockerfile:1
# StormProof server and CLI image. Build from the repository root:
#
#   docker build -f deploy/docker/Dockerfile -t stormproof .
#
# Behind a TLS-intercepting proxy, pass its CA bundle as a build secret so
# the module download can verify certificates; it is used only for that
# step and never stored in the image:
#
#   docker build --secret id=ca_bundle,src=/path/to/ca.crt -f deploy/docker/Dockerfile -t stormproof .

FROM golang:1.24.7-alpine AS build
WORKDIR /src
# Download modules first, in their own layer, so code changes don't
# re-download them.
COPY go.mod go.sum ./
RUN --mount=type=secret,id=ca_bundle,required=false \
    if [ -f /run/secrets/ca_bundle ]; then export SSL_CERT_FILE=/run/secrets/ca_bundle; fi; \
    go mod download
COPY . .
# Static binaries (no cgo), so the runtime image needs no libc beyond what
# alpine has, and builds are reproducible (-trimpath).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stormproof-server ./server/cmd/stormproof-server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stormproof-cli ./client/cmd/stormproof-cli

# alpine rather than distroless: compose health checks need a tool (wget)
# to call /healthz, and it's small either way.
FROM alpine:3.20
RUN adduser -D -u 10001 stormproof && mkdir /data && chown stormproof /data
COPY --from=build /out/ /usr/local/bin/
USER stormproof
VOLUME /data
# 7000: Raft and the client API (gRPC). 9090: metrics and probes (HTTP).
EXPOSE 7000 9090
ENTRYPOINT ["stormproof-server"]

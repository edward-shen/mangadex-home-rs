# syntax=docker/dockerfile:1

FROM rust:alpine as builder
COPY . .
RUN apk add --no-cache file make musl-dev \
  && cargo install --path . \
  && strip /usr/local/cargo/bin/mangadex-home

FROM alpine:latest
COPY --from=builder /usr/local/cargo/bin/mangadex-home /usr/local/bin/mangadex-home
CMD ["mangadex-home"]
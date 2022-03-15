#syntax=docker/dockerfile:experimental

# Build image
FROM rust:1.59-slim-bullseye as builder

COPY ./ci/install-build-deps.sh .
RUN bash install-build-deps.sh

RUN cargo new --bin crible
WORKDIR /crible

# 1. Build only dependencies against an empty app
COPY ./Cargo.lock ./Cargo.toml .
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release

# 2. Build the app itself
RUN rm src/*.rs
COPY ./src ./src
RUN rm ./target/release/deps/crible*
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release

# Runtime image
FROM debian:bullseye-slim

RUN apt-get update && apt-get install --yes --no-install-recommends \
    tini \
    curl

RUN \
    addgroup --system --gid 1000 crible && \
    adduser --system --uid 2000 --ingroup crible crible

COPY --from=builder /crible/target/release/crible /usr/local/bin/crible

WORKDIR /home/crible
EXPOSE 3000
USER crible
ENTRYPOINT ["tini", "--"]
CMD /usr/local/bin/crible

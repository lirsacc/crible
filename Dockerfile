FROM rust:1.59-slim-bullseye as builder

RUN cargo new --bin crible
WORKDIR /crible

RUN apt-get update && apt-get install --yes --no-install-recommends \
    libclang-dev

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release

RUN rm src/*.rs
COPY ./src ./src
RUN rm ./target/release/deps/crible* && cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /crible/target/release/crible .
ENTRYPOINT ["./crible"]

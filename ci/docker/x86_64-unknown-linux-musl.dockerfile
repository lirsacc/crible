FROM ghcr.io/cross-rs/x86_64-unknown-linux-musl:edge
COPY ./ci/install-build-deps.sh .
RUN bash install-build-deps.sh

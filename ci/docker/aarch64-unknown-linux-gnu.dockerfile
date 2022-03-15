FROM ghcr.io/cross-rs/aarch64-unknown-linux-gnu:edge
COPY ci/install-build-deps.sh .
RUN bash install-build-deps.sh

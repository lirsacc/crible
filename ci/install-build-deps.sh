#!/usr/bin/env bash
apt-get update
apt-get install --yes --no-install-recommends \
    libclang-dev \
    libc6-dev \
    clang \
    gcc \
    g++ \
    musl-tools

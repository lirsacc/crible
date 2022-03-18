#!/usr/bin/env bash

set -x -eu -o pipefail

pkgs=(
    # These should already be in the cross-rs docker images but we want them for
    # the raw CI environment.
    g++
    libc6-dev
    libclang-dev
)

apt_opts=(
    --assume-yes
    --no-install-recommends
)

apt-get update

apt-get install "${apt_opts[@]}" "${pkgs[@]}"

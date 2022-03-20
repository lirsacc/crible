#!/usr/bin/env bash

set -eu -o pipefail

rustup component add --toolchain nightly llvm-tools-preview
cargo +nightly install grcov

export LLVM_PROFILE_FILE="$(pwd)/target/debug/coverage-pid%p.profraw"

export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
export RUSTDOCFLAGS="-Cpanic=abort -Zunstable-options --persist-doctests target/debug/doctestbins"

HTML_OUTPUT=".coverage"
LCOV_OUTPUT=".coverage/coverage.lcov"

cargo +nightly test --workspace

rm -rf $HTML_OUTPUT

grcov \
    ./target/debug/ \
    --source-dir . \
    --llvm \
    --branch \
    --ignore-not-existing \
    --output-type html \
    --ignore 'target/debug/build/*' \
    --output-path $HTML_OUTPUT

grcov \
    ./target/debug/ \
    --source-dir . \
    --llvm \
    --branch \
    --ignore-not-existing \
    --output-type lcov \
    --ignore 'target/debug/build/*' \
    --output-path $LCOV_OUTPUT

if [[ $* == *--open* ]]; then
    open $HTML_OUTPUT/index.html
fi

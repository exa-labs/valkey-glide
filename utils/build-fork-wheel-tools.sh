#!/usr/bin/env bash
# Install pinned native build tools or the local server used by wheel tests.
set -euo pipefail

mode="$1"
root="$2"
tools=$(mktemp -d)
trap 'rm -rf "$tools"' EXIT

check_hash() {
    python -c 'import hashlib, pathlib, sys; assert hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() == sys.argv[2]' "$1" "$2"
}

case "$mode" in
    build)
        curl --fail --location --silent --show-error \
            "https://static.rust-lang.org/rustup/archive/1.28.2/$RUSTUP_TARGET/rustup-init" -o "$tools/rustup-init"
        check_hash "$tools/rustup-init" "$RUSTUP_SHA256"
        chmod +x "$tools/rustup-init"
        "$tools/rustup-init" -y --no-modify-path --profile minimal \
            --default-host "${RUSTUP_TARGET/linux-musl/linux-gnu}" --default-toolchain 1.95.0
        curl --fail --location --silent --show-error \
            "https://github.com/protocolbuffers/protobuf/releases/download/v3.20.0/protoc-3.20.0-$PROTOC_PLATFORM.zip" -o "$tools/protoc.zip"
        check_hash "$tools/protoc.zip" "$PROTOC_SHA256"
        unzip -qo "$tools/protoc.zip" -d "$HOME/.local"
        protoc --version
        protoc -Iprotobuf="$root/glide-core/src/protobuf" \
            --python_out="$root/python/glide-shared/glide_shared" \
            "$root"/glide-core/src/protobuf/*.proto
        cp "$root/python/README.md" "$root/python/glide-sync/README.md"
        printf '%s\n' 'setuptools==80.9.0' 'wheel==0.45.1' 'cffi==2.0.0' 'pycparser==2.23' > /tmp/glide-build-constraints.txt
        ;;
    test)
        curl --fail --location --silent --show-error \
            https://github.com/valkey-io/valkey/archive/refs/tags/8.0.4.tar.gz -o "$tools/valkey.tar.gz"
        check_hash "$tools/valkey.tar.gz" 55c12a25f67ef19b615c76b6cb0c92d12753d76eb8d38b31d30e299c3490cdf2
        tar -xzf "$tools/valkey.tar.gz" -C "$tools"
        make -C "$tools/valkey-8.0.4/src" -j2 MALLOC=libc valkey-server
        mkdir -p "$HOME/.local/bin"
        cp "$tools/valkey-8.0.4/src/valkey-server" "$HOME/.local/bin/valkey-server"
        ;;
    *)
        echo "Unknown mode: $mode" >&2
        exit 1
        ;;
esac

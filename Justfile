set shell := ["bash", "-euo", "pipefail", "-c"]

bpfAssetsDir := "./internal/runtime/bpf/assets"

lint:
    golangci-lint run

# Needs bpf-linker
bpf:
    cargo +nightly build \
      --manifest-path bpf/Cargo.toml \
      -p xdp-packet-drop \
      --release \
      --target bpfel-unknown-none \
      -Z build-std=core
    cp bpf/target/bpfel-unknown-none/release/xdp-packet-drop {{bpfAssetsDir}}/xdp-packet-drop.bpf.o

nuke:
    sudo ./scripts/nuke_ctr_namespace.sh pakostii || true
    sudo ./scripts/nuke_veths.sh

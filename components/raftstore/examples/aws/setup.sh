#!/usr/bin/env bash
# Post-boot setup. Run after ssh-ing into the bench host.
#   - Installs rustup + stable toolchain (per tikv rust-toolchain.toml).
#   - Builds the metronome-branch tikv-server in release mode (slow: 30-45 min on c6i.8xlarge).
#   - Installs tiup and fetches a known-good pd-server binary.
#   - Builds go-ycsb via `go install`.
# Idempotent: safe to re-run; cached cargo build picks up where it left off.
set -eux

TIDB_VER="${TIDB_VER:-v8.5.6}"

# Rust toolchain.
if ! command -v cargo >/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"

# tiup for pd-server.
if [ ! -x "$HOME/.tiup/bin/tiup" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
fi
export PATH="$HOME/.tiup/bin:$PATH"
tiup install "pd:${TIDB_VER}"

# go-ycsb. Main package lives at cmd/go-ycsb, not the repo root.
export PATH="$HOME/go/bin:$PATH"
if [ ! -x "$HOME/go/bin/go-ycsb" ]; then
    go install github.com/pingcap/go-ycsb/cmd/go-ycsb@latest
fi

# Make PATH additions persistent for future shells.
{
    echo 'export PATH="$HOME/.cargo/bin:$HOME/.tiup/bin:$HOME/go/bin:$PATH"'
} >> "$HOME/.bashrc"

# Build tikv-server (metronome branch).
cd "$HOME/tikv"
git status
cargo build --release --bin tikv-server

# Final sanity report.
echo
echo "=== Setup complete ==="
echo "  TiKV:    $HOME/tikv/target/release/tikv-server  ($(stat -c '%y' "$HOME/tikv/target/release/tikv-server"))"
echo "  PD:      $HOME/.tiup/components/pd/${TIDB_VER}/pd-server"
echo "  go-ycsb: $HOME/go/bin/go-ycsb"
echo
echo "Mounted data volumes:"
df -h --output=source,target,size,avail | grep '/data/disk' || echo "  (none mounted — check user-data log: /var/log/user-data.log)"
echo
echo "Next: bash \$HOME/bench.sh"

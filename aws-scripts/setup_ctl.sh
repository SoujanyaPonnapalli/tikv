#!/bin/bash
# Controller-side setup: install Rust + tiup + go-ycsb, then cargo build the
# tikv-server binary. After this, scp the binary to each TiKV host.
#
# Usage: bash setup_ctl.sh "<tikv-host-1> <tikv-host-2> ..."  (space-sep IPs)
set -ex

# Rust toolchain.
if ! command -v cargo >/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
source "$HOME/.cargo/env"

# tiup for PD binary.
if [ ! -x "$HOME/.tiup/bin/tiup" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
fi
export PATH="$HOME/.tiup/bin:$PATH"
tiup install pd:v8.5.6

# go-ycsb from source (the `go install` path is blocked by replace directives).
export PATH="$HOME/go/bin:$PATH"
if [ ! -x "$HOME/go/bin/go-ycsb" ]; then
    if [ ! -d "$HOME/go-ycsb-src" ]; then
        git clone https://github.com/pingcap/go-ycsb.git "$HOME/go-ycsb-src"
    fi
    (cd "$HOME/go-ycsb-src" && git fetch --all && mkdir -p "$HOME/go/bin" \
        && go build -o "$HOME/go/bin/go-ycsb" ./cmd/go-ycsb)
fi

# Build tikv-server.
cd "$HOME/tikv"
git pull --ff-only
cargo build --release --bin tikv-server

# Distribute the binary + tomls to TiKV hosts.
if [ -n "${1:-}" ]; then
    SSH_KEY="$HOME/.ssh/tikv-bench.pem"
    if [ ! -f "$SSH_KEY" ]; then
        # Use the EC2 metadata to find the instance's key pair, fall back to no auth.
        echo "no $SSH_KEY found; assuming controller can reach tikv hosts via instance ssh agent" >&2
    fi
    SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"
    [ -f "$SSH_KEY" ] && SSH_OPTS="$SSH_OPTS -i $SSH_KEY"
    for ip in $1; do
        echo "  scp tikv-server + tomls to $ip"
        scp $SSH_OPTS "$HOME/tikv/target/release/tikv-server" "ubuntu@$ip:/home/ubuntu/tikv-server"
        scp $SSH_OPTS "$HOME/tikv/aws-scripts/tikv-baseline.toml" \
                       "$HOME/tikv/aws-scripts/tikv-metronome.toml" \
                       "ubuntu@$ip:/home/ubuntu/"
        ssh $SSH_OPTS "ubuntu@$ip" 'chmod +x ~/tikv-server'
    done
fi

echo
echo "=== ctl setup complete ==="
echo "  tikv-server: $HOME/tikv/target/release/tikv-server  (+ pushed to TiKV hosts)"
echo "  pd-server:   $HOME/.tiup/components/pd/v8.5.6/pd-server"
echo "  go-ycsb:     $HOME/go/bin/go-ycsb"

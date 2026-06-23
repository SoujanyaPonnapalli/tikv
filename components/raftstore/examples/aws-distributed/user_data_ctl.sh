#!/bin/bash
# Controller cloud-init: install OS deps + clone metronome branch.
# Builds + benches run after SSH login via setup.sh / bench_knee_distributed.py.
set -ex
exec > /var/log/user-data.log 2>&1

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake libssl-dev pkg-config protobuf-compiler \
    git curl wget jq golang-go awscli netcat-openbsd python3-pip
pip3 install --quiet paramiko

sudo -u ubuntu git clone https://github.com/soujanyaponnapalli/tikv.git /home/ubuntu/tikv
sudo -u ubuntu git -C /home/ubuntu/tikv fetch --all
sudo -u ubuntu git -C /home/ubuntu/tikv checkout metronome
sudo -u ubuntu git -C /home/ubuntu/tikv pull --ff-only

# Symlink convenience scripts.
sudo -u ubuntu ln -sf /home/ubuntu/tikv/components/raftstore/examples/aws-distributed/setup_ctl.sh /home/ubuntu/setup_ctl.sh
sudo -u ubuntu ln -sf /home/ubuntu/tikv/components/raftstore/examples/aws-distributed/bench_knee_distributed.py /home/ubuntu/bench_knee_distributed.py

touch /var/log/user-data-done

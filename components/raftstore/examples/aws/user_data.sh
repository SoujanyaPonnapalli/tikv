#!/bin/bash
# Cloud-init bootstrap. Runs once on first boot as root.
# - Installs OS deps for TiKV build + bench.
# - Formats every attached non-root NVMe device as ext4.
# - Mounts them at /data/disk1 .. /data/diskN in alphabetical device order.
# - Clones the user's tikv metronome branch so the bench scripts and tomls
#   land at /home/ubuntu/tikv/components/raftstore/examples/aws/.
#
# Build of tikv-server is NOT done here — kept in setup.sh so it runs visibly
# after SSH and isn't hidden in cloud-init logs.
set -eux
exec > /var/log/user-data.log 2>&1

# Wait for all attached EBS volumes to settle.
sleep 30

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
    build-essential cmake libssl-dev pkg-config protobuf-compiler libprotobuf-dev \
    git curl wget jq unzip ca-certificates \
    golang-go \
    awscli \
    htop iotop sysstat

# Format and mount every non-root NVMe device.
# On Nitro instances the root EBS is /dev/nvme0n1; data volumes show up as
# /dev/nvme1n1, /dev/nvme2n1, ... in arbitrary order. We don't care about the
# /dev/sdX-to-nvmeN mapping because all data volumes are configured identically.
mkdir -p /data
i=1
for dev in /dev/nvme[1-9]n1 /dev/nvme1[0-9]n1; do
    [ -b "$dev" ] || continue
    if blkid "$dev" >/dev/null 2>&1; then
        continue
    fi
    mkfs.ext4 -F "$dev"
    mkdir -p "/data/disk$i"
    mount -o noatime,nodiratime "$dev" "/data/disk$i"
    echo "$(blkid -s UUID -o value "$dev")  /data/disk$i  ext4  defaults,noatime,nodiratime  0 0" >> /etc/fstab
    chown -R ubuntu:ubuntu "/data/disk$i"
    i=$((i+1))
done

# Clone the user's tikv fork on the metronome branch so the bench scripts and
# tomls are available at a stable path. setup.sh and bench.sh both live under
# components/raftstore/examples/aws/.
sudo -u ubuntu -H bash -lc '
    set -eux
    cd /home/ubuntu
    if [ ! -d tikv ]; then
        git clone https://github.com/soujanyaponnapalli/tikv.git
    fi
    cd tikv
    git fetch --all
    git checkout metronome
    git pull --ff-only
'

# Convenience symlinks so the user can just `bash setup.sh` after ssh.
sudo -u ubuntu ln -sf /home/ubuntu/tikv/components/raftstore/examples/aws/setup.sh /home/ubuntu/setup.sh
sudo -u ubuntu ln -sf /home/ubuntu/tikv/components/raftstore/examples/aws/bench.sh /home/ubuntu/bench.sh

touch /var/log/user-data-done
echo "user_data complete"

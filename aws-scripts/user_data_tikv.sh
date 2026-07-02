#!/bin/bash
# TiKV host cloud-init: mount the dedicated gp3 data volume + install minimum
# deps. The tikv-server binary is scp'd in from the controller; this host
# never builds anything.
set -ex
exec > /var/log/user-data.log 2>&1

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y libgomp1 libssl3

# Wait for the data volume to attach.
sleep 20

# Find the non-root nvme volume (root is nvme0n1).
for dev in /dev/nvme[1-9]n1; do
  [ -b "$dev" ] || continue
  if blkid "$dev" >/dev/null 2>&1; then continue; fi
  mkfs.ext4 -F "$dev"
  mkdir -p /data/disk
  mount -o noatime,nodiratime "$dev" /data/disk
  echo "$dev /data/disk ext4 defaults,noatime,nodiratime 0 0" >> /etc/fstab
  chown -R ubuntu:ubuntu /data/disk
  break
done

# Allow controller to push files + run remote ssh commands as `ubuntu`.
# Both hosts share the same AWS key pair, so the controller's private key
# pairs with the public key already in /home/ubuntu/.ssh/authorized_keys.

touch /var/log/user-data-done

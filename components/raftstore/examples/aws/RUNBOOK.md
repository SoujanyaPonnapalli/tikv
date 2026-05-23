# AWS disk-bandwidth-bound TiKV metronome benchmark

End-to-end workflow for the matrix bench
(N in {3,5,7} x val in {256, 1024, 4096} x {baseline, metronome})
under a real disk-bandwidth ceiling (125 MiB/s gp3 per TiKV node).

This is the disk-bound counterpart to the localhost matrix in
`../metronome_matrix_bench.sh`. Phase 7 showed K/N savings invisible on
local NVMe; this setup pins each tikv-server to its own gp3 volume so
the raft-log fsync path is the bottleneck.

## Files in this directory
- `main.tf` — Terraform: 1 EC2 host + 7 gp3 EBS volumes + security group.
- `user_data.sh` — cloud-init: installs OS deps, formats/mounts disks, clones the metronome branch.
- `setup.sh` — post-SSH: installs rust + tiup + go-ycsb, builds `tikv-server`.
- `bench.sh` — runs the full matrix, writes `matrix-results.csv`.
- `tikv-baseline.toml`, `tikv-metronome.toml` — per-cell server configs.

## Prereqs
- AWS credentials configured (`aws sts get-caller-identity` returns your account).
- Terraform >= 1.5.
- An EC2 key pair in your target region. Create one:
  ```sh
  aws ec2 create-key-pair --key-name tikv-bench --region us-east-1 \
      --query 'KeyMaterial' --output text > ~/.ssh/tikv-bench.pem
  chmod 400 ~/.ssh/tikv-bench.pem
  ```
- The metronome branch on origin must include this directory. Push first if needed:
  ```sh
  cd /Users/soujanya/Projects/current/tikv
  git add components/raftstore/examples/aws/
  git commit -m "AWS disk-bandwidth-bound bench harness"
  git push origin metronome
  ```
  `user_data.sh` clones from `github.com/soujanyaponnapalli/tikv` and checks out `metronome`.

## Workflow

```sh
cd components/raftstore/examples/aws/

# 1. Configure.
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars                          # set key_name and ssh_cidr

# 2. Provision.
terraform init
terraform apply                                   # eyeball the plan, then yes
PUBLIC_IP=$(terraform output -raw public_ip)

# 3. Wait for cloud-init.
ssh -i ~/.ssh/tikv-bench.pem ubuntu@$PUBLIC_IP \
    'until [ -f /var/log/user-data-done ]; do sleep 5; done; echo ready'

# 4. Build + install tooling (one-shot; about 30-45 min on c6i.8xlarge).
ssh -i ~/.ssh/tikv-bench.pem ubuntu@$PUBLIC_IP 'bash ~/setup.sh'

# 5. Run the matrix (~20-30 min).
ssh -i ~/.ssh/tikv-bench.pem ubuntu@$PUBLIC_IP 'bash ~/bench.sh'

# 6. Pull the results.
scp -i ~/.ssh/tikv-bench.pem \
    ubuntu@$PUBLIC_IP:/data/disk1/bench/matrix-results.csv \
    ./matrix-results-ebs125.csv

# 7. Tear down so you stop paying.
terraform destroy
```

## Expected ballpark cost
- c6i.8xlarge: about $1.36/hr in us-east-1
- 7 gp3 100 GiB volumes at 125 MiB/s default: about $0.11/hr (no throughput surcharge at the default)
- Full session (provision + build + bench + destroy): about 1.5 hours, so roughly $2-3.

## What to look for in the results
- **OPS / latency delta**: at val=4096 with N=5 or N=7, metronome should beat
  baseline by a factor close to N/K (1.67x at N=5,K=3; 2.33x at N=7,K=4). If
  it doesn't, suspect CPU or PD as the new bottleneck before disk.
- **`tikv_raftstore_metronome_entries_skipped_total`**: this counter (logged
  per cell at the end of `run_cell`) should climb to roughly `(N-K)/N` of total
  written entries. If it's zero or close to it, the filter isn't engaging and
  the bench is invalid for the metronome cells.
- **iostat -x 5** on the host during a run: per-volume %util should sit near
  100% on the baseline cells and drop on the metronome cells — that's the
  K/N saving made visible at the device layer.

## Tuning knobs (env vars to bench.sh)
- `THREADS=400` — push concurrency higher if 200 clients don't saturate.
- `RUN_OPS=500000` — longer runs cut p99 noise (phase-7 saw 3x cell-to-cell
  variance at 10k ops; 100k is the new default but 500k is cleaner).
- `RECORDS=50000` — bigger key space; relevant only if region splits start
  helping spread writes across volumes.

# Distributed TiKV knee bench (one TiKV per VM)

Parallel to the colocated `aws/` bench, but each `tikv-server` runs on its
own EC2 instance with its own gp3 EBS volume. Inter-replica RPC goes over
real (same-AZ, cluster-placement-group) network instead of loopback. A
single controller host runs PD + go-ycsb + the orchestrator.

## Topology
- 1 × controller (`c6i.4xlarge`): PD + go-ycsb + bench driver. Root EBS only.
- N × TiKV hosts (`c6i.4xlarge`): each owns a dedicated gp3 data volume
  (100 GiB, 125 MB/s, 3000 IOPS).
- One cluster placement group keeps all hosts on the same physical rack
  (~ sub-ms RTT).

## Prereqs
- AWS creds + Terraform 1.5+.
- EC2 key pair `tikv-bench` in your region.
- `~/.ssh/tikv-bench.pem` locally and the same key uploaded to the
  controller during setup.

## Workflow

```sh
cd components/raftstore/examples/aws-distributed/

# 1. Provision.
cat > terraform.tfvars <<EOF
key_name = "tikv-bench"
ssh_cidr = "<YOUR_IP>/32"
num_tikv = 3
EOF
terraform init
terraform apply -auto-approve

CTL=$(terraform output -raw ctl_public_ip)
TIKV_IPS=$(terraform output -json tikv_private_ips | jq -r '. | join(" ")')

# 2. Wait for both cloud-inits to finish (~1 min).
ssh -i ~/.ssh/tikv-bench.pem ubuntu@$CTL \
    'until [ -f /var/log/user-data-done ]; do sleep 5; done; echo ctl-init-done'

# 3. Copy the ssh key onto the controller so it can ssh to TiKV hosts.
scp -i ~/.ssh/tikv-bench.pem ~/.ssh/tikv-bench.pem ubuntu@$CTL:~/.ssh/tikv-bench.pem
ssh  -i ~/.ssh/tikv-bench.pem ubuntu@$CTL 'chmod 400 ~/.ssh/tikv-bench.pem'

# 4. Build + distribute tikv-server (cargo, then scp to each TiKV host).
ssh  -i ~/.ssh/tikv-bench.pem ubuntu@$CTL "bash ~/setup_ctl.sh '$TIKV_IPS'"

# 5. Run the bench.
ssh  -i ~/.ssh/tikv-bench.pem ubuntu@$CTL \
    "python3 ~/bench_knee_distributed.py --tikv-ips $TIKV_IPS \
        --threads 64 128 256 512 1024 2048 4096 --val 4096"

# 6. Pull the CSV.
scp  -i ~/.ssh/tikv-bench.pem ubuntu@$CTL:/data/bench-knee-dist/knee-results.csv \
     ./knee-distributed-results.csv

# 7. Tear down.
terraform destroy -auto-approve
```

## Cost (us-east-1, on-demand)
- Controller `c6i.4xlarge`: ~$0.68/hr
- N TiKV `c6i.4xlarge`: N × ~$0.68/hr
- N gp3 100 GB: N × ~$0.01/hr (125 MB/s baseline; no extra throughput charge)

At N=3 a full sweep (~30 min) costs ≈ $1.40.

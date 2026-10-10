# Deploying lsm-rust on AWS

`lsm-rust` is an embedded, single-writer storage engine fronted by a RESP
server. It runs as **one pod with one EBS volume**; there is no clustering or
replication, so durability comes from the volume (take EBS snapshots, e.g. with
AWS Backup) and availability from fast rescheduling. Scale by sharding keys in
the application, not by raising replicas.

## 1. Image registry (ECR)

The ECR repository and the GitHub OIDC push role live in the separate
`lsm-infra` repository (`aws-ecr/` module), since they are account-level
infrastructure rather than part of the engine. Apply it there, then set the
repository variables `AWS_ECR_PUSH_ROLE_ARN`, `AWS_REGION` and
`AWS_ECR_REPOSITORY` here and the release workflow mirrors each signed image to
ECR. Without them the workflow publishes to GHCR only.

## 2. Cluster prerequisites (EKS)

- **EBS CSI driver** add-on and a `gp3` StorageClass (set `persistence.storageClass`).
- **VPC CNI network policy** enabled (or Calico/Cilium) if `networkPolicy.enabled`.
- Pod Security Admission `restricted` on the namespace — the chart complies.
- A way to deliver the password: External Secrets Operator or the Secrets Store
  CSI driver syncing AWS Secrets Manager into a Secret with key `password`.

## 3. Install

```sh
helm upgrade --install lsm deploy/helm/lsm-rust -n lsm --create-namespace \
  --set image.repository=<account>.dkr.ecr.<region>.amazonaws.com/lsm-rust \
  --set image.tag=0.1.0 \
  --set auth.existingSecret=lsm-rust-auth
```

Prefer pinning `image.digest` and verifying the signature first:

```sh
cosign verify ghcr.io/zvdy/lsm-rust@sha256:... \
  --certificate-identity-regexp 'https://github.com/zvdy/lsm-rust/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Security posture

| Concern | Control |
|---|---|
| Authentication | RESP `AUTH`, password read from a mounted file (never argv/env); constant-time compare |
| Transport | Plaintext RESP: keep inside the VPC; use an **internal** NLB or ClusterIP and a TLS-terminating proxy if clients are outside the cluster |
| Network | NetworkPolicy limits RESP to labelled clients, metrics to the monitoring namespace |
| Runtime | Non-root (65532), read-only root FS, all capabilities dropped, seccomp `RuntimeDefault`, no service-account token, distroless image |
| Resource abuse | Connection cap, idle timeout, bounded request framing |
| Supply chain | `--locked` builds, cargo-deny/audit, Trivy scan, cosign-signed image, SBOM + provenance, immutable ECR tags |
| Data | gp3 volume, enable EBS encryption (KMS) on the StorageClass (`encrypted: "true"`) |

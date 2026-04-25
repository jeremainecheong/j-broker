# j-broker Helm chart

a minimal Helm 3 chart for deploying j-broker on Kubernetes. Ships
a 3-replica broker StatefulSet, a single-replica admin UI Deployment, an
optional bundled Redis for quota + SSE fan-out, and opt-in mTLS wiring
that consumes the certs produced by `scripts/tls/bootstrap-ca.sh`.

## Topology

```mermaid
flowchart TB
    subgraph Outside[Outside cluster]
        Operator((Operator))
        Browser((Browser))
    end

    subgraph K8s[Kubernetes namespace]
        subgraph Broker[StatefulSet · broker × 3]
            B0[broker-0]
            B1[broker-1]
            B2[broker-2]
        end

        subgraph PVC[PersistentVolumeClaims]
            PVC0[(broker-0 data<br/>10 GiB)]
            PVC1[(broker-1 data<br/>10 GiB)]
            PVC2[(broker-2 data<br/>10 GiB)]
        end

        AdminDep[Deployment · admin-app × 1+]
        RedisDep[Deployment · redis<br/>opt-in]

        BSvc[Headless Service<br/>broker · :9092 / :9093]
        ASvc[Service · admin · :15672]
        RSvc[Service · redis · :6379]
        ING[Ingress · admin<br/>opt-in]

        Secret[(Secret · jbroker-tls<br/>ca.crt + tls.crt + tls.key<br/>opt-in)]
    end

    B0 -.mounts.-> PVC0
    B1 -.mounts.-> PVC1
    B2 -.mounts.-> PVC2
    B0 & B1 & B2 -.tls.enabled.-> Secret
    AdminDep -.tls.enabled.-> Secret
    AdminDep -.redis.enabled.-> RedisDep

    BSvc --> B0 & B1 & B2
    ASvc --> AdminDep
    RSvc --> RedisDep
    ING --> ASvc

    Operator -->|kubectl port-forward| ASvc
    Browser -->|when ingress enabled| ING
```

Key points: brokers are a StatefulSet (stable network identity for Raft voter config), admin is a Deployment (stateless, scale up + enable Redis pub/sub for multi-pod SSE fan-out). Redis and TLS are opt-in via values.

## Quick start

```bash
# Build + load the images into your local cluster (Kind / minikube):
docker build -f Dockerfile.broker -t jbroker-broker:1.4.0 .
docker build -f Dockerfile.admin  -t jbroker-admin:1.4.0  .
kind load docker-image jbroker-broker:1.4.0 jbroker-admin:1.4.0

# Install with defaults (3-broker plaintext cluster, no Redis, no TLS):
helm install jb deploy/helm/j-broker

# Port-forward the admin UI:
kubectl port-forward svc/jb-j-broker-admin 15672:15672
open http://localhost:15672
```

## Values

See [`values.yaml`](./values.yaml) for the authoritative list.
Highlights:

| Key | Default | Purpose |
|---|---|---|
| `broker.replicaCount` | `3` | Raft-majority-friendly odd number. |
| `broker.persistence.size` | `10Gi` | Per-broker PVC size. |
| `broker.advertisedHostTemplate` | `""` | `sprintf`-style template used to build `--advertised-listeners` for external clients. Empty → skip the flag. |
| `admin.replicaCount` | `1` | Bump + enable Redis for multi-pod SSE fan-out (). |
| `admin.ingress.enabled` | `false` | Stand up an Ingress for the admin UI. |
| `tls.enabled` | `false` | Turn on mTLS on every gRPC hop. |
| `tls.secretName` | `jbroker-tls` | Kubernetes Secret carrying `ca.crt` + `tls.crt` + `tls.key` PEM files. |
| `redis.enabled` | `false` | Bundled Redis Deployment for quota + SSE fan-out. |

## Enabling mTLS

```bash
scripts/tls/bootstrap-ca.sh .tls

kubectl create secret generic jbroker-tls \
    --from-file=tls.crt=.tls/broker1.crt \
    --from-file=tls.key=.tls/broker1.key \
    --from-file=ca.crt=.tls/ca.crt

helm upgrade --install jb deploy/helm/j-broker \
    --set tls.enabled=true \
    --set tls.secretName=jbroker-tls
```

Note: the dev script generates per-broker server certs under names
`broker1.crt`/`broker2.crt`/`broker3.crt`. The chart mounts one Secret on
every broker pod — in a production deployment you'd issue one cert per
StatefulSet pod (SAN-matching each pod's stable DNS name) and use a
`volumeClaimTemplate` / cert-manager issuer instead.

## Smoke tests

```bash
deploy/helm/j-broker/tests/render-smoke.sh
```

Runs `helm template` across the default, TLS-enabled, Redis-enabled, and
advertised-listener value matrices and grep-asserts structural
invariants. No Kubernetes cluster required.

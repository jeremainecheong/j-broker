# j-broker operations runbooks

Runbooks for the six failure modes an operator actually meets. Every alert
name, metric, command, and endpoint referenced here exists in this repo —
where a symptom has no backing alert yet, the runbook says so instead of
inventing one.

Related files:

- Alert pack + thresholds + the needs-metric ledger:
  [`deploy/helm/j-broker/values.yaml`](../helm/j-broker/values.yaml)
  (`metrics.prometheusRule`), rendered by
  [`deploy/helm/j-broker/templates/prometheusrule.yaml`](../helm/j-broker/templates/prometheusrule.yaml).
- Grafana dashboards:
  [`scripts/monitoring/grafana/dashboards/cluster-overview.json`](../../scripts/monitoring/grafana/dashboards/cluster-overview.json)
  and
  [`scripts/monitoring/grafana/dashboards/partitions.json`](../../scripts/monitoring/grafana/dashboards/partitions.json),
  provisioned by `docker-compose.monitoring.yml` for local stacks.
- Metric source: the admin-app is the single scrape point. Its
  `MetricsScraper` fans `DescribeMetrics` out to every broker every 5s
  (`jbroker.metrics.scrape.intervalSeconds`) and re-exposes the merged
  snapshot at `/actuator/prometheus`
  (`admin-app/src/main/java/jbroker/admin/api/PrometheusMetricsBinder.java`).
  Broker pods have no metrics port of their own.

Conventions used below:

- Helm release `jb`, so resources render as `jb-j-broker-broker`
  (StatefulSet), `jb-j-broker-broker-headless` (Service),
  `jb-j-broker-admin` (Deployment + Service). Substitute your release name.
- `j-broker` = `broker-app/build/install/broker-app/bin/broker-app`. The
  admin CLI defaults to `--admin http://localhost:9090`; against the chart,
  port-forward first and pass `--admin http://localhost:15672`:

  ```bash
  kubectl -n <ns> port-forward svc/jb-j-broker-admin 15672:15672
  ```

- Broker pod ordinal N runs broker id N+1 (`jb-j-broker-broker-0` is
  broker 1).

---

## 1. Broker down

A single broker pod is dead or unreachable while the rest of the cluster
keeps serving.

### Symptoms

- `jbroker_broker_scrape_ok{broker_id="N"} == 0` — the broker stopped
  answering the admin's `DescribeMetrics` fan-out. This is the direct
  signal; note the caveat that all *other* per-broker gauges freeze at
  their last-reported values while the broker is absent from the scrape.
- Admin UI health badge (`GET /api/v1/health/badge`, polled by every page)
  goes **yellow** ("majority up but degraded") for one dead broker, **red**
  once a majority is down or no Raft leader exists.
- `broker_fenced` event on the admin SSE stream (`GET /api/v1/events`).
- After 10 minutes, `JBrokerUnderReplicatedPartitions` fires for every
  partition whose ISR shrank below its replica set.
- No dedicated single-broker-down alert ships yet (see the ledger in
  `values.yaml`); `jbroker_broker_scrape_ok` is the metric an
  `== 0 for: 5m` rule would use.

### Diagnosis

```bash
kubectl -n <ns> get pods -l app.kubernetes.io/component=broker
kubectl -n <ns> describe pod jb-j-broker-broker-N     # events, probe failures, OOMKilled
kubectl -n <ns> logs jb-j-broker-broker-N --previous  # why the last process died
j-broker admin --admin http://localhost:15672 cluster-info   # alive flags + controller id
```

The probes are TCP on the gRPC port — the port only binds after segment
recovery and Raft init, so a pod stuck in `startupProbe` with a large data
dir may simply be replaying logs (budget: 5 minutes).

### Remediation

Failover is automatic: the active controller's `BrokerFencer` (250ms tick,
3s staleness threshold) demotes the dead broker's partition leaderships to
a surviving ISR member with a bumped leader epoch, within ~3.5s of the
last heartbeat. You are restoring capacity, not availability — unless the
dead broker was the sole ISR member of some partition (see runbook 3).

- Crash/OOM: `kubectl -n <ns> delete pod jb-j-broker-broker-N` — the
  StatefulSet recreates it on the same PVC; it replays its log, catches up
  via replication, and rejoins ISRs on its own.
- Node failure: wait for rescheduling; anti-affinity is soft, so a
  replacement node is not strictly required.
- Data volume lost: delete the pod's PVC and pod together. A fresh broker
  with the same id and an empty data dir re-replicates everything it
  followed from the surviving leaders (`DeadNodeReplaceIT` covers this
  path end to end).

### Prevention

- Keep the chart's PodDisruptionBudget (`maxUnavailable: 1`) and
  anti-affinity enabled — they cap voluntary evictions below Raft-majority
  loss.
- Add an alert on `jbroker_broker_scrape_ok == 0` once you run the alert
  pack; the gauge exists, the rule does not ship yet.

---

## 2. Disk full

A broker's data volume drops below the headroom watermark.

### Symptoms

- Producers receive retriable `STORAGE_FULL` errors. Fetch, replication,
  offset commits, and admin RPCs keep serving — the degradation is
  produce-only by design.
- Broker log: `data volume below headroom: <X> usable < <Y> watermark —
  refusing client produces`.
- `jbroker_disk_headroom_low{broker_id="N"} == 1` while the broker is
  refusing produces; `jbroker_disk_usable_bytes{broker_id="N"}` trends
  toward the watermark first ("Disk usable bytes per broker" panel on the
  cluster-overview dashboard).
- No shipped alert (ledger). A rule on `jbroker_disk_usable_bytes` below a
  byte threshold, or `jbroker_disk_headroom_low == 1`, would back this.

### Diagnosis

```bash
kubectl -n <ns> exec jb-j-broker-broker-N -- df /var/lib/jbroker
kubectl -n <ns> get pvc -l app.kubernetes.io/component=broker
j-broker admin --admin http://localhost:15672 topics list   # find the space hogs
```

The watermark is `storage.headroom.bytes` (default 1 GiB, env
`JBROKER_STORAGE_HEADROOM_BYTES`); the probe re-checks every 10s, so
produces resume automatically on the first probe after space frees.

### Remediation

In order of preference:

1. Tighten retention on the offending topics — per-topic `retention.ms` /
   `retention.bytes` override the cluster defaults; the cleaner ticks
   every 5 minutes (`log.cleaner.interval.ms`).
2. Force-compact compacted topics now instead of waiting for the cleaner:
   `POST /api/v1/topics/{name}/partitions/{p}/compact` (also a button on
   the admin UI partition view).
3. Delete topics you can afford to lose:
   `j-broker admin topics delete --topic NAME`.
4. Expand the PVC (`kubectl -n <ns> patch pvc data-jb-j-broker-broker-N`)
   if the StorageClass allows volume expansion.

### Prevention

- Size `broker.persistence.size` against the retention budget: with
  `retention.bytes` set, each partition converges to
  `[retention.bytes, retention.bytes + segment.bytes)`.
- Alert on `jbroker_disk_usable_bytes` well above the watermark so you act
  before produces degrade.

---

## 3. Offline partition

Every ISR member of a partition is down; the controller has set
`leader = -1`.

### Symptoms

- `acks=all` produces to the partition are rejected — the produce path
  treats `leader = -1` as "reject write".
- `j-broker admin topics describe --topic T` shows `"leader": -1` for the
  partition; the admin UI topic page shows a "no leader" pill in place of
  the leader badge and "HWM / LEO unavailable — partition has no live
  leader". The ISR list is **preserved** — it records which replicas hold
  every committed record.
- **No alert, and no metric.** Partition gauges (`jbroker_isr_size`,
  `jbroker_hwm`, `jbroker_leader_log_end_offset`) are leader-reported, so
  a leaderless partition silently disappears from the exposition — its
  series go stale rather than going to zero. The ledger in `values.yaml`
  calls this out; a controller-side offline-partitions gauge is the
  missing metric. Until then the alert you will actually see is whatever
  preceded the outage (`JBrokerUnderReplicatedPartitions`, broker-down
  symptoms from runbook 1).

### Diagnosis

```bash
j-broker admin --admin http://localhost:15672 topics describe --topic T   # leader, isr, replicas
kubectl -n <ns> get pods -l app.kubernetes.io/component=broker            # which ISR members are down
```

Map ISR broker ids to pods (id N = pod ordinal N-1) and find the one you
can revive fastest.

### Remediation

Bring back **any preserved-ISR member**. Recovery is automatic: the same
fencer tick that fenced the leader scans offline partitions and, as soon
as a preserved-ISR member heartbeats again, proposes it as leader with a
bumped epoch. Nothing to trigger manually.

Do **not** "fix" this by wiping the returning replica's data dir or
recreating the topic: the partition is unavailable-not-inconsistent, and
the preserved ISR is what guarantees no acked record is lost. A wiped
replica cannot be promoted safely.

### Prevention

- Replication factor ≥ 3 with `min.insync.replicas: 2` (the cluster
  default) keeps produces failing fast (`NOT_ENOUGH_REPLICAS`) before the
  last replica dies, instead of after.
- Act on `JBrokerUnderReplicatedPartitions` and `JBrokerReplicationLagHigh`
  — an offline partition is almost always preceded by a shrinking ISR.
- Note the ledger's second gap: `NOT_ENOUGH_REPLICAS` produce rejections
  are returned to clients but never counted, so min-ISR pressure has no
  metric either.

---

## 4. Lagging consumer group

A group's committed offsets fall ever further behind the high watermark.

### Symptoms

- Downstream consumers process stale data; lag grows monotonically.
- **No backing metric or alert.** The Prometheus exposition carries
  replication lag (`jbroker_replication_lag_records` — follower vs leader),
  not consumer lag. Group lag is only visible through the admin read
  surface below; a per-group lag gauge in `DescribeMetrics` is what an
  alert would need.

### Diagnosis

```bash
j-broker admin --admin http://localhost:15672 groups list
j-broker admin --admin http://localhost:15672 groups describe --group ID
```

`groups describe` (= `GET /api/v1/consumer-groups/{id}`) returns per
partition: committed offset, high watermark, lag, and the owning member
id. Read it twice a minute apart:

- HWM advancing, committed offset flat → consumer stuck or crash-looping.
- Both advancing, gap widening → consumers healthy but underprovisioned.
- A partition with no owner member → group is mid-rebalance or a member
  died; check `state` and member count in `groups list`.

The admin UI consumer-groups page renders the same data; the
`consumer_group_rebalance` SSE event fires on generation changes.

### Remediation

- Underprovisioned: add members, up to the partition count of the
  subscribed topics; beyond that, add partitions.
- Stuck on a poison record: skip it with an offset reset —
  `POST /api/v1/consumer-groups/{id}/reset-offsets` with
  `{"resets": [{"topic": "T", "partition": 0, "offset": <new>}]}`
  (per-partition error codes come back in the response). Stop the
  consumers first; a live member's next commit overwrites the reset.
- Start over: `DELETE /api/v1/consumer-groups/{id}` drops the group and
  its offsets; the next join re-forms it fresh.

### Prevention

- Watch lag operationally via the group-describe surface until a lag
  gauge exists in the exposition.
- Mind `offsets.retention.ms` (default 7 days): a group idle longer than
  that loses its commits, and the bundled consumer client falls back to
  the log start — which reads as "sudden huge lag" plus reprocessing at
  first glance.

---

## 5. Certificate expiry

The mTLS server or client certs in the `tls.secretName` Secret expire.

### Symptoms

- Every gRPC hop fails its TLS handshake at once: clients, inter-broker
  replication, and the admin-app's broker dials. Broker logs show
  handshake failures; nothing is wrong with the processes themselves.
- Admin UI health badge goes red with
  `admin unable to reach any broker: ...`; every
  `jbroker_broker_scrape_ok` gauge drops to 0 **while the admin's
  `/actuator/prometheus` endpoint stays up** — `JBrokerAdminMetricsDown`
  does *not* fire, because Prometheus scrapes the admin over plain HTTP.
- If replication died before client traffic, `JBrokerReplicationStalled`
  fires (HWM stuck with records past it).
- No cert-expiry metric or alert exists. Expiry dates live only in the
  certs.

### Diagnosis

```bash
kubectl -n <ns> get secret jbroker-tls -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -noout -enddate -subject
kubectl -n <ns> get secret jbroker-tls -o jsonpath='{.data.ca\.crt}' \
  | base64 -d | openssl x509 -noout -enddate
```

The dev bootstrap scripts mint 825-day server/client certs under a
3650-day CA, so the server cert expires first.

### Remediation

The rotation procedure is printed by the chart itself
(`deploy/helm/j-broker/templates/NOTES.txt`); same-CA rotation is the
common case and keeps every existing client working — proven by
`CertRotationIT`:

```bash
# 1. Fresh server cert under the same CA (script keeps ca.key/ca.crt):
scripts/tls/bootstrap-k8s.sh .tls-k8s jb-j-broker-broker-headless <ns>

# 2. Replace the Secret in place:
kubectl -n <ns> create secret generic jbroker-tls \
    --from-file=tls.crt=.tls-k8s/server.crt \
    --from-file=tls.key=.tls-k8s/server.key \
    --from-file=ca.crt=.tls-k8s/ca.crt \
    --dry-run=client -o yaml | kubectl apply -f -

# 3. Roll the brokers — certs are read at process start, so pods must
#    restart to pick up the new Secret. SIGTERM drains partition
#    leadership (shutdown.timeout.ms, 30s budget) before each exit, so
#    acks=all clients see retriable errors only:
kubectl -n <ns> rollout restart statefulset/jb-j-broker-broker
kubectl -n <ns> rollout status  statefulset/jb-j-broker-broker

# 4. Restart the admin deployment the same way:
kubectl -n <ns> rollout restart deployment/jb-j-broker-admin
```

Rotating the **CA itself** needs the two-step trust dance: ship a
`ca.crt` bundle containing old+new CAs and roll; switch server certs to
the new CA and roll again; then drop the old CA from the bundle.

### Prevention

- Calendar the expiry dates when you mint certs; there is no metric that
  will warn you.
- For production, issue per-pod certs from a real issuer (cert-manager)
  instead of the bootstrap scripts — see the note in
  `deploy/helm/j-broker/README.md`.
- Rehearse the roll: it is the same procedure as any rolling restart, and
  the PDB (`maxUnavailable: 1`) plus leadership draining make it safe
  under load.

---

## 6. Full-cluster cold start

Every broker is down — planned (maintenance window) or not (power loss) —
and the cluster must come back from disk.

### What to expect

- The StatefulSet uses `podManagementPolicy: Parallel` on purpose: a
  three-voter Raft election cannot complete until at least two pods are
  up, so ordered startup would deadlock on first deploy. Start everything
  at once.
- On boot each broker CRC-verifies every segment and truncates at the
  last intact batch — torn frames from the crash are dropped and logged
  (position, bytes dropped, resume offset). Committed records are not
  lost: durability comes from replication, and the preserved ISR decides
  leadership.
- The `format.version` marker at the data-dir root makes a rolling
  *downgrade* onto newer data fail loudly instead of corrupting silently.
  A broker that refuses to boot after a version change is this, not
  corruption.
- The startup probe allows 5 minutes of recovery before liveness
  interferes; large data dirs replay for a while.

### Procedure

```bash
# 1. Start (or un-scale) the brokers; PVCs retain all state:
kubectl -n <ns> scale statefulset/jb-j-broker-broker --replicas=3
kubectl -n <ns> rollout status statefulset/jb-j-broker-broker

# 2. Confirm a controller was elected and all brokers registered:
j-broker admin --admin http://localhost:15672 raft           # one LEADER, terms converged
j-broker admin --admin http://localhost:15672 cluster-info   # controllerId > 0, every node alive

# 3. Spot-check state:
j-broker admin --admin http://localhost:15672 topics list
j-broker admin --admin http://localhost:15672 groups list
```

Health badge green = controller known and every registered broker alive.
Watch the cluster-overview dashboard: `jbroker_raft_current_term` should
step once and hold; a climbing term means the quorum keeps re-electing
(`JBrokerRaftTermFlapping` fires if it exceeds the threshold).

### Restoring from cold backup

If a data dir is lost or corrupt beyond the automatic truncation, restore
the cold backup (`BackupRestoreDrillIT` automates this exact drill):

```bash
# Backups are taken from a cleanly stopped broker: copy the entire data
# dir — partition logs AND Raft state together, never separately.

# Gate the restore on the offline integrity check (exits 1 on any
# corrupt batch, so scripts can gate on it):
j-broker admin verify-log /path/to/backup/data

# Then boot the broker on the copy with the same id and ports.
```

### Prevention

- Run `verify-log` against every backup when you take it, not when you
  need it.
- Keep an odd `broker.replicaCount`; changing it on a running release
  requires manual Raft voter reconfiguration (see the comment in
  `values.yaml`).
- Prefer controlled shutdown (SIGTERM, leadership drain) over `kill -9`
  when taking a cluster down for maintenance — recovery is then a replay
  of clean logs.

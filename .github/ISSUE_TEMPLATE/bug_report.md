---
name: Bug report
about: Something behaves incorrectly
labels: bug
---

**Broker version / commit**
The image tag (e.g. `jbroker-broker:1.4.0`), the `broker_version` string from the `ApiVersions` handshake, or — if built from source — the git commit.

**Cluster shape**
Single broker, 3-node compose, or Helm; auth mode; any configuration that differs from defaults.

**What happened**

**Steps to reproduce**
Exact commands or client calls, in order.

**Expected behaviour**

**Logs**
Broker logs around the failure. For consistency bugs, include partition state (`topics describe` or the topic detail page: ISR, HWM, LEO).

# k6 load tests

Three scripts, the spec / success criterion #9. `admin-api-smoke` and `admin-api-load` run on stock k6; `sse-connections` imports `k6/x/sse` and needs a binary built with [xk6-sse](https://github.com/phymbert/xk6-sse):

```bash
mkdir -p bin && docker run --rm -v "$PWD/bin:/xk6" grafana/xk6 build --with github.com/phymbert/xk6-sse
```

Run against the compose cluster (`docker compose up -d`, admin on :15672):

```bash
docker run --rm -v "$PWD/scripts/k6:/scripts" --add-host=host.docker.internal:host-gateway \
  grafana/k6 run -e ADMIN_URL=http://host.docker.internal:15672 /scripts/admin-api-smoke.js

# sse variant uses the xk6 binary:
docker run --rm -v "$PWD/bin:/xk6" -v "$PWD/scripts/k6:/scripts" \
  --add-host=host.docker.internal:host-gateway --entrypoint /xk6/k6 \
  grafana/k6 run -e ADMIN_URL=http://host.docker.internal:15672 /scripts/sse-connections.js
```

## Last verified run — 2026-07-07 (M-series laptop, Docker 27.4, fresh 3-broker compose cluster)

| Script | Shape | Thresholds | Result |
|---|---|---|---|
| `admin-api-load.js` | 100 VUs · 5m · read endpoints | p99 < 500ms, err < 1% | **PASS** — 789,936 reqs @ 2,401 req/s, p99 = 46.6ms, err 0.05% |
| `sse-connections.js` | ramp to 1000 SSE clients · 5m hold | no drops, ≥1 event/VU | **PASS** — 519,000 events @ 1,403/s to 1000 held connections, zero errors |
| `admin-api-smoke.js` | 10 VUs · 1m · topic CRUD | p95 < 100ms, err < 1% | **PASS** — 22,210 reqs, p95 = 21.6ms, err 0.00%, 4,442/4,442 checks |

Two findings from these runs, both fixed:

- The smoke exposed a ~5s p95 cliff on every admin mutation routed to a Raft follower — a follower's propose is silently dropped, so the handler burned its full `proposeAndWait` timeout before returning NOT_LEADER. Fixed by AdminHandler's leadership fast path.
- `sse-connections.js`'s per-VU "received an event" check recorded zero samples (any `check()` after the blocking `sse.open` never runs during the hold), passing its threshold vacuously. The check now fires from inside the event handler.

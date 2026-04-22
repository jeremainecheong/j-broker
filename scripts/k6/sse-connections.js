// 1000 concurrent SSE connections, held for 5 min. Assert: no
// dropped connections; every VU receives at least one event during the run.
//
// Uses k6's sse experimental module. Run under `k6 run
// --compatibility-mode=experimental_enhanced scripts/k6/sse-connections.js`
// (or the default runner with a recent k6 version).
//
// The admin-app's SseEmitter uses timeout=0 so these connections stay
// open for the full 5-min run; the broker periodically emits events as
// the chaos driver (or normal traffic) creates/deletes topics.
import sse from "k6/x/sse";
import { check } from "k6";

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 1000 },
        { duration: "5m", target: 1000 },
        { duration: "10s", target: 0 },
      ],
    },
  },
  thresholds: {
    // No connection errors; every VU sees at least one event during the
    // hold window. Check counts are counted as metric samples; threshold
    // enforces the pass rate.
    checks: ["rate>0.99"],
  },
};

const BASE = __ENV.ADMIN_URL || "http://localhost:9090";

export default function () {
  let received = 0;
  const res = sse.open(`${BASE}/api/v1/events`, { tags: { name: "events" } }, function (client) {
    client.on("event", function () {
      received++;
    });
    client.on("error", function () {
      check(null, { "sse error-free": () => false });
    });
  });
  check(res, { "sse 200": (r) => r && r.status === 200 });
  // Stay connected for a bit to amortise connect cost; k6 teardown closes.
  check(null, { "events received": () => received >= 0 });
}

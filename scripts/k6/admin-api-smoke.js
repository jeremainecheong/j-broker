// Acceptance gate: `k6 run scripts/k6/admin-api-smoke.js` passes thresholds.
//
// VUs=10, 1 min. List / describe / create / delete on random topics.
// Thresholds: p95 < 100ms, 0 errors.
import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "1m",
  thresholds: {
    http_req_duration: ["p(95)<100"],
    http_req_failed: ["rate<0.01"],
  },
};

const BASE = __ENV.ADMIN_URL || "http://localhost:9090";

export default function () {
  http.get(`${BASE}/api/v1/cluster`);
  http.get(`${BASE}/api/v1/topics`);
  const name = `smoke-${__VU}-${__ITER}`;
  const createRes = http.post(
    `${BASE}/api/v1/topics`,
    // admin API JSON envelope is snake_case.
    JSON.stringify({ name, partitions: 1, replication_factor: 1, config: {} }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(createRes, { "create 2xx": (r) => r.status === 201 });
  http.get(`${BASE}/api/v1/topics/${name}`);
  http.del(`${BASE}/api/v1/topics/${name}`);
  sleep(0.1);
}

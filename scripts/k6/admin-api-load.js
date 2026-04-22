// sustained read-heavy admin traffic. VUs=100, 5 min. p99 < 500 ms.
import http from "k6/http";
import { sleep } from "k6";

export const options = {
  vus: 100,
  duration: "5m",
  thresholds: {
    http_req_duration: ["p(99)<500"],
    http_req_failed: ["rate<0.01"],
  },
};

const BASE = __ENV.ADMIN_URL || "http://localhost:9090";

export default function () {
  // Read-heavy mix — cluster, topics, groups, raft, metrics.
  http.get(`${BASE}/api/v1/cluster`);
  http.get(`${BASE}/api/v1/topics`);
  http.get(`${BASE}/api/v1/consumer-groups`);
  http.get(`${BASE}/api/v1/raft`);
  http.get(`${BASE}/api/v1/metrics/throughput`);
  http.get(`${BASE}/api/v1/metrics/latency`);
  sleep(0.05);
}

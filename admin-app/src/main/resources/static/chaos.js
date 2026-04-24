// Chaos page client. Three responsibilities:
//   1. Paint the live topology SVG — nodes colored by role + chaos state,
//      partition edges rendered dashed red between blocked peer pairs.
//      Polls /api/v1/cluster + /api/v1/chaos/state every 3 s.
//   2. Register the chaosFeed Alpine component via the alpine:init event
//      (UI audit #26: previously chaosFeed was a bare global defined in
//      this deferred script, which Alpine's auto-init couldn't see because
//      defer-execution order vs DOMContentLoaded isn't guaranteed to let
//      bare globals win the race).
//   3. Decorate the flash region after every htmx swap so action results
//      are visually attributed to their button.

// ---- 1. Topology SVG --------------------------------------------------

const TOPOLOGY_POLL_MS = 3000;
const SVG_NS = "http://www.w3.org/2000/svg";

function ringLayout(n, cx, cy, r) {
  const out = [];
  for (let i = 0; i < n; i++) {
    // Start at -π/2 so the first broker sits at the top and equilateral
    // triangles read naturally (b1 at 12 o'clock).
    const theta = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    out.push({ x: cx + r * Math.cos(theta), y: cy + r * Math.sin(theta) });
  }
  return out;
}

function nodeClass(node, chaos) {
  // Chaos-caused states (paused, latency) take precedence over the cluster's
  // liveness signal: a paused broker's heartbeats stop so it APPEARS dead to
  // peers, but the operator just pressed "Pause" and expects to see yellow,
  // not red. Pair with the text badge ("paused" / "123ms") for full context.
  if (chaos && chaos.available && chaos.paused) return "paused";
  if (chaos && chaos.available && chaos.latency_ms > 0) return "latency";
  if (!node.alive) return "dead";
  if (node.role === "LEADER") return "leader";
  return "follower";
}

// Text shown inside the node circle under the id. Chaos-caused states
// win (paused beats dead/role) so the color + text agree. Long Raft
// roles (PRE_CANDIDATE, CANDIDATE) are abbreviated so they fit within
// the r=42 circle without clipping.
function roleLabel(node, chaos) {
  if (chaos && chaos.available && chaos.paused) return "paused";
  if (chaos && chaos.available && chaos.latency_ms > 0) return "latency";
  if (!node.alive) return "dead";
  const r = (node.role || "").toLowerCase();
  if (r === "pre_candidate") return "pre-cand";
  if (r === "candidate") return "candidate";
  return r;
}

function isPartitioned(chaos, peerId) {
  if (!chaos) return false;
  return (
    (chaos.outbound_blocked_peers && chaos.outbound_blocked_peers.includes(peerId)) ||
    (chaos.inbound_blocked_peers && chaos.inbound_blocked_peers.includes(peerId))
  );
}

function chaosBadge(chaos) {
  if (!chaos || !chaos.available) return "";
  const parts = [];
  if (chaos.paused) parts.push("paused");
  if (chaos.latency_ms > 0) parts.push(chaos.latency_ms + "ms");
  const bp =
    (chaos.outbound_blocked_peers || []).length +
    (chaos.inbound_blocked_peers || []).length;
  if (bp > 0) parts.push("partitioned");
  return parts.join(" · "); // middle dot
}

function clearChildren(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
}

function renderTopology(cluster, chaosByBroker) {
  const svg = document.getElementById("chaos-topology");
  if (!svg) return;
  const edges = svg.querySelector("#chaos-edges");
  const nodes = svg.querySelector("#chaos-nodes");
  const empty = svg.querySelector("#chaos-topology-empty");
  if (empty) empty.remove();

  // Rebuild every tick. Cluster layout is tiny (3 nodes typically) so
  // the "diff the DOM" complexity isn't worth it; wholesale redraw
  // keeps chaos.js dead simple to reason about.
  clearChildren(edges);
  clearChildren(nodes);

  const list = cluster.nodes || [];
  if (list.length === 0) return;

  const positions = ringLayout(list.length, 300, 160, 110);
  const posById = new Map();
  list.forEach((n, i) => posById.set(n.broker_id, positions[i]));

  // Edges first so nodes draw on top. Emit one line per (a, b) pair
  // regardless of direction — dashed red communicates "something's
  // wrong in this pair" without needing to encode direction visually.
  const rendered = new Set();
  list.forEach((a) => {
    const ca = chaosByBroker.get(a.broker_id);
    list.forEach((b) => {
      if (a.broker_id >= b.broker_id) return;
      const key = a.broker_id + "-" + b.broker_id;
      if (rendered.has(key)) return;
      const cb = chaosByBroker.get(b.broker_id);
      const blocked = isPartitioned(ca, b.broker_id) || isPartitioned(cb, a.broker_id);
      if (!blocked) return;
      const pa = posById.get(a.broker_id);
      const pb = posById.get(b.broker_id);
      const line = document.createElementNS(SVG_NS, "line");
      line.setAttribute("x1", pa.x);
      line.setAttribute("y1", pa.y);
      line.setAttribute("x2", pb.x);
      line.setAttribute("y2", pb.y);
      line.setAttribute("class", "chaos-edge partition");
      edges.appendChild(line);
      rendered.add(key);
    });
  });

  list.forEach((n) => {
    const pos = posById.get(n.broker_id);
    const chaos = chaosByBroker.get(n.broker_id);
    const g = document.createElementNS(SVG_NS, "g");
    g.setAttribute("class", "chaos-topology-node");
    g.setAttribute("data-broker-id", n.broker_id);
    g.setAttribute("transform", "translate(" + pos.x + "," + pos.y + ")");

    const circle = document.createElementNS(SVG_NS, "circle");
    circle.setAttribute("r", 42);
    circle.setAttribute("class", "node-shape " + nodeClass(n, chaos));
    g.appendChild(circle);

    const label = document.createElementNS(SVG_NS, "text");
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("class", "node-label");
    label.setAttribute("y", -4);
    label.textContent = "b" + n.broker_id;
    g.appendChild(label);

    const role = document.createElementNS(SVG_NS, "text");
    role.setAttribute("text-anchor", "middle");
    role.setAttribute("class", "node-role");
    role.setAttribute("y", 12);
    role.textContent = roleLabel(n, chaos);
    g.appendChild(role);

    const badge = chaosBadge(chaos);
    if (badge) {
      const b = document.createElementNS(SVG_NS, "text");
      b.setAttribute("text-anchor", "middle");
      b.setAttribute("class", "node-chaos-badge");
      b.setAttribute("y", 62);
      b.textContent = badge;
      g.appendChild(b);
    }

    // Accessible tooltip.
    const title = document.createElementNS(SVG_NS, "title");
    title.textContent =
      "broker " + n.broker_id +
      " — " + (n.alive ? (n.role || "?") : "DEAD") +
      (badge ? " · " + badge : "");
    g.appendChild(title);

    nodes.appendChild(g);
  });
}

// Single-flight + trailing-call pattern. SSE events fire frequently
// (broker_registered per heartbeat, chaos_injected on every action);
// without this gate we'd issue hundreds of concurrent fetch() per
// minute and trip the browser's max-connections-per-origin limit
// (ERR_INSUFFICIENT_RESOURCES). At most one request is in flight;
// callers during a pending flight get coalesced into one trailing
// refresh on completion.
let _pollInFlight = false;
let _pollPending = false;
async function pollTopology() {
  if (_pollInFlight) {
    _pollPending = true;
    return;
  }
  _pollInFlight = true;
  try {
    const [cluster, chaos] = await Promise.all([
      fetch("/api/v1/cluster").then((r) => (r.ok ? r.json() : null)),
      fetch("/api/v1/chaos/state").then((r) => (r.ok ? r.json() : null))
    ]);
    if (cluster) {
      const chaosByBroker = new Map();
      if (chaos && Array.isArray(chaos.brokers)) {
        chaos.brokers.forEach((b) => chaosByBroker.set(b.broker_id, b));
      }
      renderTopology(cluster, chaosByBroker);
    }
  } catch (e) {
    // Network blip — next tick retries.
  } finally {
    _pollInFlight = false;
    if (_pollPending) {
      _pollPending = false;
      // schedule instead of recursing so the callback can't starve the
      // event loop if events are arriving faster than fetches complete
      setTimeout(pollTopology, 0);
    }
  }
}

// ---- 2. Alpine live-events component ----------------------------------

// Register via alpine:init so the component is resolvable the moment
// Alpine walks x-data attributes. Pre-fix chaosFeed was a bare global in
// this deferred script, which led to "chaosFeed is not defined" console
// errors because Alpine initialized before chaos.js had finished loading.
document.addEventListener("alpine:init", () => {
  window.Alpine.data("chaosFeed", () => ({
    events: [],
    init() {
      const es = new EventSource("/api/v1/events");
      const self = this;
      // Only chaos + leader events should trigger an immediate
      // repaint — broker_registered/heartbeat events fire on every
      // tick and would cause a fetch storm. The 3 s setInterval
      // catches anything we skip here.
      const eagerRefreshTypes = new Set([
        "leader_changed",
        "chaos_injected",
        "chaos_healed",
        "kill",
        "pause",
        "resume",
        "partition",
        "heal-partition",
        "inject-latency",
        "force-election",
      ]);
      const push = (type, payload) => {
        const rec = {
          id: (payload && payload.id) || Date.now() + Math.random(),
          type: type,
          body: JSON.stringify(payload),
          time: new Date().toLocaleTimeString(),
        };
        self.events.unshift(rec);
        if (self.events.length > 50) self.events.pop();
        if (eagerRefreshTypes.has(type)) pollTopology();
      };
      [
        "broker_registered",
        "leader_changed",
        "group_rebalanced",
        "chaos_injected",
        "chaos_healed",
        "kill",
        "pause",
        "resume",
        "partition",
        "heal-partition",
        "inject-latency",
        "force-election",
      ].forEach((t) =>
        es.addEventListener(t, (evt) => {
          try {
            push(t, JSON.parse(evt.data));
          } catch (e) {
            push(t, { raw: evt.data });
          }
        })
      );
      es.onmessage = (evt) => {
        try {
          push("event", JSON.parse(evt.data));
        } catch (e) {
          push("event", { raw: evt.data });
        }
      };
    },
  }));
});

// ---- 3. htmx flash decoration ----------------------------------------

document.body.addEventListener("htmx:afterSwap", function (evt) {
  if (!evt.detail.target || evt.detail.target.id !== "chaos-flash") return;
  const ok = evt.detail.xhr.status >= 200 && evt.detail.xhr.status < 300;
  evt.detail.target.classList.remove("ok", "err");
  evt.detail.target.classList.add(ok ? "ok" : "err");
  clearTimeout(window.__chaosFlashTimer);
  window.__chaosFlashTimer = setTimeout(function () {
    evt.detail.target.classList.remove("ok", "err");
    evt.detail.target.textContent = "";
  }, 4000);
  // Action almost certainly moved topology state. Refresh.
  pollTopology();
});

// ---- boot ------------------------------------------------------------

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    pollTopology();
    setInterval(pollTopology, TOPOLOGY_POLL_MS);
  });
} else {
  pollTopology();
  setInterval(pollTopology, TOPOLOGY_POLL_MS);
}

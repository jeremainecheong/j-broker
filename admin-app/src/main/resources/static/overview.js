// P11.3 — overview dashboard client. Two responsibilities:
//   1. Poll /api/v1/metrics/throughput every 2s, keep a rolling ring of
//      30 samples per metric (≈ 1 min of history), redraw inline SVG
//      sparklines.
//   2. Update the "current" text next to each sparkline with the most
//      recent sample.
//
// The server does not persist historical samples; every browser keeps its
// own short history. That's fine for an admin dashboard — an operator who
// cares about longer windows is already on Grafana.

(function () {
  var HISTORY = 30;
  var INTERVAL_MS = 2000;
  var series = { produce: [], fetch: [] };

  function formatBps(v) {
    if (v < 1024) return v.toFixed(0) + " B/s";
    if (v < 1024 * 1024) return (v / 1024).toFixed(1) + " KiB/s";
    return (v / (1024 * 1024)).toFixed(2) + " MiB/s";
  }

  // Produce an SVG path string from a numeric series. The viewport is
  // fixed 300×60; samples are right-aligned so the latest tick is on the
  // right edge. Separate stroke path (just the polyline) and fill path
  // (polyline + baseline closure) for the subtle shaded look.
  function buildPaths(values) {
    if (values.length === 0) {
      return { stroke: "M0,60", fill: "M0,60 L300,60 L300,60 L0,60 Z" };
    }
    var max = Math.max.apply(null, values);
    if (max <= 0) max = 1;
    var width = 300;
    var height = 60;
    var step = width / Math.max(1, HISTORY - 1);
    var firstIdx = Math.max(0, HISTORY - values.length);
    var pts = values.map(function (v, i) {
      var x = (firstIdx + i) * step;
      var y = height - (v / max) * (height - 2) - 1; // 1px headroom at top
      return [x, y];
    });
    var stroke = "M" + pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" L");
    var first = pts[0];
    var last = pts[pts.length - 1];
    var fill =
      "M" + first[0].toFixed(1) + "," + height +
      " L" + pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" L") +
      " L" + last[0].toFixed(1) + "," + height + " Z";
    return { stroke: stroke, fill: fill };
  }

  function redraw(kind) {
    var svg = document.querySelector('svg[data-sparkline="' + kind + '"]');
    if (!svg) return;
    var paths = buildPaths(series[kind]);
    svg.querySelector(".sparkline-stroke").setAttribute("d", paths.stroke);
    svg.querySelector(".sparkline-fill").setAttribute("d", paths.fill);
  }

  function updateCurrent(kind, value) {
    var el = document.querySelector('[data-spark-current="' + kind + '"]');
    if (el) el.textContent = formatBps(value);
  }

  function tick() {
    fetch("/api/v1/metrics/throughput")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var p = d.produceBytesPerSec || 0;
        var f = d.fetchBytesPerSec || 0;
        pushSample("produce", p);
        pushSample("fetch", f);
        updateCurrent("produce", p);
        updateCurrent("fetch", f);
        redraw("produce");
        redraw("fetch");
      })
      .catch(function () {
        // Silent — overview page stays rendered; next tick will retry.
      });
  }

  function pushSample(kind, v) {
    series[kind].push(v);
    if (series[kind].length > HISTORY) series[kind].shift();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      tick();
      setInterval(tick, INTERVAL_MS);
    });
  } else {
    tick();
    setInterval(tick, INTERVAL_MS);
  }
})();

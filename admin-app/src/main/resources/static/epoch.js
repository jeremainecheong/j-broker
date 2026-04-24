/*
 * Client-side relative-time rendering for <time data-epoch="…"> elements.
 *
 * Server templates emit raw epoch-millis into the datetime attribute and
 * the element text. On DOMContentLoaded we walk every such element and
 * rewrite the text to a human-friendly "Ns ago / Nm ago / Nh ago / Nd ago"
 * label. If JS is disabled the raw-fallback text still renders, so the
 * table degrades gracefully rather than showing a gap.
 */
(function () {
  function formatRelative(ms) {
    var diff = Date.now() - ms;
    if (isNaN(diff) || diff < 0) return 'just now';
    var s = Math.floor(diff / 1000);
    if (s < 5) return 'just now';
    if (s < 60) return s + 's ago';
    var m = Math.floor(s / 60);
    if (m < 60) return m + 'm ago';
    var h = Math.floor(m / 60);
    if (h < 24) return h + 'h ago';
    var d = Math.floor(h / 24);
    return d + 'd ago';
  }
  function render() {
    document.querySelectorAll('time[data-epoch]').forEach(function (el) {
      var raw = Number(el.getAttribute('data-epoch'));
      if (!Number.isFinite(raw) || raw <= 0) return;
      el.textContent = formatRelative(raw);
    });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', render);
  } else {
    render();
  }
})();

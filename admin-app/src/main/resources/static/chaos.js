// P11.6 — chaos page client. Two responsibilities:
//   1. Subscribe to the existing /api/v1/events SSE stream, keep a bounded
//      reverse-chronological list of up to 50 events for the right-rail.
//   2. Decorate the flash region (#chaos-flash) after every htmx swap so
//      the user sees whether the last action succeeded. The flash content
//      comes from the server response (hx-swap="textContent") so no DOM
//      injection is performed here — we only toggle classes and clear
//      after 4s.

function chaosFeed() {
  return {
    events: [],
    subscribe() {
      var es = new EventSource("/api/v1/events");
      var push = (type, payload) => {
        var rec = {
          id: (payload && payload.id) || Date.now() + Math.random(),
          type: type,
          body: JSON.stringify(payload),
          time: new Date().toLocaleTimeString(),
        };
        this.events.unshift(rec);
        if (this.events.length > 50) this.events.pop();
      };
      [
        "broker_registered",
        "leader_changed",
        "group_rebalanced",
        "chaos_injected",
        "chaos_healed",
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
  };
}

document.body.addEventListener("htmx:afterSwap", function (evt) {
  if (!evt.detail.target || evt.detail.target.id !== "chaos-flash") return;
  var ok = evt.detail.xhr.status >= 200 && evt.detail.xhr.status < 300;
  evt.detail.target.classList.remove("ok", "err");
  evt.detail.target.classList.add(ok ? "ok" : "err");
  clearTimeout(window.__chaosFlashTimer);
  window.__chaosFlashTimer = setTimeout(function () {
    evt.detail.target.classList.remove("ok", "err");
    // textContent (not innerHTML) to avoid any possibility of re-rendering
    // residual markup as live DOM.
    evt.detail.target.textContent = "";
  }, 4000);
});

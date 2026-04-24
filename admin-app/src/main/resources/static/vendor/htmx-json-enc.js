// htmx json-enc extension — self-hosted (BSD-Zero-Clause, from htmx-extensions).
// Converts form-encoded bodies into application/json before htmx fires the
// request so our REST endpoints can deserialize via @RequestBody records.
// Pre-audit this shipped from unpkg; self-hosting closes the external dep
// + removes the flaky CDN load order.
(function () {
  htmx.defineExtension("json-enc", {
    onEvent: function (name, evt) {
      if (name === "htmx:configRequest") {
        evt.detail.headers["Content-Type"] = "application/json";
      }
    },
    encodeParameters: function (xhr, parameters, elt) {
      xhr.overrideMimeType("text/json");
      var out = {};
      Object.keys(parameters).forEach(function (k) {
        var v = parameters[k];
        // Coerce numeric-looking strings (e.g. select options) into Number so
        // the REST envelope records (PartitionBody.from: int) bind cleanly.
        if (typeof v === "string" && v !== "" && !isNaN(Number(v))) {
          out[k] = Number(v);
        } else {
          out[k] = v;
        }
      });
      return JSON.stringify(out);
    }
  });
})();

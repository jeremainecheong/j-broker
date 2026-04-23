// P11.2 — admin UI client glue. Intentionally minimal.
//
// Responsibilities:
//   1. Highlight the current-page nav link by matching window.location.pathname
//      against each link's href (longest-prefix wins so /topics/hello still
//      lights up /topics).
//
// Everything else is driven by htmx (badge polling) + Thymeleaf server-render.

(function () {
  function markActiveNav() {
    var path = window.location.pathname;
    var links = document.querySelectorAll("nav.topnav a[data-nav]");
    var bestLink = null;
    var bestLen = -1;
    for (var i = 0; i < links.length; i++) {
      var href = links[i].getAttribute("href");
      if (!href) continue;
      // Boundary-aware prefix match: /topics must match /topics or
      // /topics/<anything> but NOT /topics-archive. "/" is the root — exact
      // match only, otherwise it would shadow every nested route.
      var matches =
        href === "/"
          ? path === "/"
          : path === href || path.indexOf(href + "/") === 0;
      if (matches && href.length > bestLen) {
        bestLen = href.length;
        bestLink = links[i];
      }
    }
    if (bestLink) bestLink.classList.add("active");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", markActiveNav);
  } else {
    markActiveNav();
  }
})();

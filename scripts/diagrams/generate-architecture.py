#!/usr/bin/env python3
"""
Generates docs/diagrams/architecture.drawio in AWS-reference style.

Layout discipline (no-crossing routing):
  * Two columns: LEFT rail (sources), CENTER cluster.
  * Every horizontal arrow has its OWN unique Y row.
  * Every vertical arrow has its OWN unique X column.
  * Admin → brokers is ONE arrow into the data plane (not three fan-outs).
  * All edges use orthogonalEdgeStyle (right-angle routing).

Visual discipline:
  * No top header / no bottom legend — diagram speaks for itself.
  * Trust info ("mTLS internal · TLS browser") lives in the cluster title.
  * Numbered step badge baked into the edge label as inline HTML so it
    can never split the descriptive text.
  * Source boxes follow a two-color palette:
      - blue   = service traffic (PROD / CONS / BrokerClient)
      - purple = operator traffic (Browser / CLI)
  * Sub-panel borders darker than cluster background → clear hierarchy.
"""

from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
ICONS = ROOT / "docs" / "icons"
OUT = ROOT / "docs" / "diagrams" / "architecture.drawio"


# ---------- helpers --------------------------------------------------------

def svg_data_uri(name: str) -> str:
    svg = (ICONS / f"{name}.svg").read_text(encoding="utf-8")
    return "data:image/svg+xml," + quote(svg, safe="")


def xml_escape(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))


FONT = "fontFamily=ui-sans-serif, system-ui, -apple-system, sans-serif"


def panel(fill: str, stroke: str, fg: str = "#0f172a", left: bool = False) -> str:
    align = "align=left;spacingLeft=18" if left else "align=center"
    return (f"rounded=1;arcSize=4;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=2;{align};verticalAlign=top;"
            f"fontSize=14;fontStyle=1;{FONT};fontColor={fg};"
            f"spacingTop=10;shadow=0;")


def cluster_style(fill: str, stroke: str) -> str:
    return (f"rounded=1;arcSize=2;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=3;align=left;verticalAlign=top;"
            f"fontSize=15;fontStyle=1;{FONT};fontColor=#0f172a;"
            f"spacingLeft=14;spacingTop=10;shadow=0;")


LABEL = (f"text;html=1;align=center;verticalAlign=middle;fontSize=11;"
         f"fontStyle=0;{FONT};fontColor=#334155;")
SMALL = (f"text;html=1;align=center;verticalAlign=middle;fontSize=10;"
         f"fontStyle=0;{FONT};fontColor=#64748b;")


def icon_style(uri: str) -> str:
    return f"shape=image;html=1;imageAspect=1;image={uri};"


def actor_style(color: str = "#475569") -> str:
    return (f"shape=actor;html=1;fillColor={color};strokeColor={color};")


def app_box(fill: str, stroke: str, fg: str = "#0f172a") -> str:
    return (f"rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=2;fontColor={fg};fontSize=12;"
            f"fontStyle=1;{FONT};verticalAlign=middle;align=center;shadow=0;")


# Edge style — HTML labels with white background masking the line.
ARROW = (
    "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;"
    "html=1;labelBackgroundColor=#ffffff;labelBorderColor=#cbd5e1;"
    "exitDx=0;exitDy=0;entryDx=0;entryDy=0;"
    "endArrow=classic;endFill=1;endSize=8;"
    "strokeColor=#475569;strokeWidth=2;"
    f"fontSize=11;fontColor=#0f172a;{FONT};"
)
BIDI_ARROW = ARROW + "startArrow=classic;startFill=1;startSize=8;"
DASHED_ARROW = ARROW + "dashed=1;dashPattern=6 4;"


def numbered_label(n: int, text: str) -> str:
    badge = (
        f'<span style="background:#2563eb;color:#ffffff;padding:1px 7px;'
        f'border-radius:10px;font-weight:700;font-size:11px;">{n}</span>'
    )
    return f"{badge}&nbsp;&nbsp;{text}"


# ---------- cell + edge builders ------------------------------------------

cells: list[str] = []


def cell(_id: str, value: str, style: str, x: int, y: int, w: int, h: int,
         parent: str = "1") -> None:
    cells.append(
        f'<mxCell id="{_id}" value="{xml_escape(value)}" '
        f'style="{xml_escape(style)}" '
        f'vertex="1" parent="{parent}">'
        f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" '
        f'as="geometry"/></mxCell>'
    )


def edge(_id: str, source: str, target: str, label: str = "",
         style: str = ARROW,
         exit_x: float = 1, exit_y: float = 0.5,
         entry_x: float = 0, entry_y: float = 0.5) -> None:
    s = (style + f"exitX={exit_x};exitY={exit_y};"
                 f"entryX={entry_x};entryY={entry_y};")
    cells.append(
        f'<mxCell id="{_id}" value="{xml_escape(label)}" '
        f'style="{xml_escape(s)}" '
        f'edge="1" source="{source}" target="{target}" parent="1">'
        f'<mxGeometry relative="1" as="geometry"/></mxCell>'
    )


# ---------- icon URIs ------------------------------------------------------

JAVA = svg_data_uri("java")
GRPC = svg_data_uri("grpc")
SPRING = svg_data_uri("spring")
THYMELEAF = svg_data_uri("thymeleaf")
ALPINE = svg_data_uri("alpinejs")
CHARTJS = svg_data_uri("chartjs")
DOCKER = svg_data_uri("docker")
GRAFANA = svg_data_uri("grafana")
PROMETHEUS = svg_data_uri("prometheus")
REDIS = svg_data_uri("redis")
PROTOBUF = svg_data_uri("protobuf")
GH_ACTIONS = svg_data_uri("githubactions")
JUNIT = svg_data_uri("junit")


# ---------- canvas (no top header, no bottom legend) ----------------------

W, H = 1720, 960

# Two-color source palette
SVC_FILL, SVC_STROKE, SVC_FG = "#dbeafe", "#3b82f6", "#1e3a8a"   # service traffic
OPS_FILL, OPS_STROKE, OPS_FG = "#ede9fe", "#7c3aed", "#4c1d95"   # operator traffic


# ============================================================================
# LEFT RAIL — Actors + Service clients
# ============================================================================

LEFT_X, LEFT_W = 20, 280

# ---- Actors panel (Operator + Browser stacked, smaller actor figure)
cell("actors", "Actors", panel("#f8fafc", "#94a3b8"),
     LEFT_X, 20, LEFT_W, 220)

cell("op_actor", "", actor_style("#475569"),
     LEFT_X + 123, 60, 30, 44)
cell("op_lbl", "Operator", LABEL,
     LEFT_X + 30, 108, LEFT_W - 60, 18)

# Browser — caption removed (arrow already says "HTTP + SSE")
cell("browser", "🌐  Browser", app_box(OPS_FILL, OPS_STROKE, OPS_FG),
     LEFT_X + 40, 150, LEFT_W - 80, 46)

# ---- Service clients panel — every client at its own Y row.
# Tightened height: ends shortly after CLI (no wasted space).
cell("clients", "Service clients & tooling",
     panel("#f8fafc", "#94a3b8"),
     LEFT_X, 240, LEFT_W, 460)

PROD_Y, CONS_Y, JAVA_Y, CLI_Y = 290, 390, 490, 590

# Service-client boxes: width 240 (was 220) — fixes BrokerClient truncation
BOX_W = LEFT_W - 40

cell("prod_svc", "PROD  ·  Producer service",
     app_box(SVC_FILL, SVC_STROKE, SVC_FG),
     LEFT_X + 20, PROD_Y, BOX_W, 50)
cell("prod_svc_lbl", "BrokerClient · idempotent",
     SMALL, LEFT_X + 20, PROD_Y + 52, BOX_W, 16)

cell("cons_svc", "CONS  ·  Consumer service",
     app_box(SVC_FILL, SVC_STROKE, SVC_FG),
     LEFT_X + 20, CONS_Y, BOX_W, 50)
cell("cons_svc_lbl", "FindCoordinator · Fetch · Commit",
     SMALL, LEFT_X + 15, CONS_Y + 52, BOX_W + 10, 16)

# BrokerClient SDK — same blue family + Java icon
cell("javacli_box", "", app_box(SVC_FILL, SVC_STROKE, SVC_FG),
     LEFT_X + 20, JAVA_Y, BOX_W, 50)
cell("javacli_icon", "", icon_style(JAVA),
     LEFT_X + 30, JAVA_Y + 5, 40, 40)
cell("javacli_text", "BrokerClient (Java SDK)",
     "text;html=1;align=center;verticalAlign=middle;fontSize=12;fontStyle=1;"
     f"{FONT};fontColor={SVC_FG};",
     LEFT_X + 70, JAVA_Y, BOX_W - 50, 50)
cell("javacli_lbl", "gRPC · Protobuf · acks={0,1,all}",
     SMALL, LEFT_X + 15, JAVA_Y + 52, BOX_W + 10, 16)

# CLI — same purple family as Browser (operator tool, not service traffic)
cell("cli", "⌨  j-broker CLI", app_box(OPS_FILL, OPS_STROKE, OPS_FG),
     LEFT_X + 20, CLI_Y, BOX_W, 50)
cell("cli_lbl", "produce · consume · admin · chaos",
     SMALL, LEFT_X + 10, CLI_Y + 52, BOX_W + 20, 16)


# ============================================================================
# CENTER — j-broker cluster (trust info baked into the title)
# ============================================================================

CL_X, CL_Y, CL_W, CL_H = 320, 20, 1380, 920

cell("cluster",
     "j-broker cluster &nbsp; · &nbsp; 🔒 <b>mTLS</b> internal links "
     "&nbsp; · &nbsp; <b>TLS</b> for browser",
     cluster_style("#f1f5f9", "#475569"),
     CL_X, CL_Y, CL_W, CL_H)


# ---- Admin plane sub-panel (top of cluster)
ADMIN_Y = CL_Y + 50
ADMIN_H = 210

cell("admin_panel", "Admin plane  ·  Spring Boot on :15672",
     panel("#ffffff", "#94a3b8", left=True),
     CL_X + 20, ADMIN_Y, CL_W - 40, ADMIN_H)

ADMIN_APP_X = CL_X + 50
ADMIN_APP_Y = ADMIN_Y + 50
ADMIN_APP_W = 800
ADMIN_APP_H = 140

cell("admin_app", "admin-app",
     panel("#fefce8", "#eab308"),
     ADMIN_APP_X, ADMIN_APP_Y, ADMIN_APP_W, ADMIN_APP_H)


def admin_tech(idx: int, _id: str, icon_uri: str | None, label: str,
               htmx_text: bool = False) -> None:
    bx = ADMIN_APP_X + 50 + idx * 115
    by = ADMIN_APP_Y + 35
    if htmx_text:
        cell(_id, "htmx",
             "rounded=1;arcSize=20;fillColor=#3a6ea5;strokeColor=none;"
             f"fontColor=#ffffff;fontSize=14;fontStyle=1;{FONT};html=1;"
             "align=center;verticalAlign=middle;",
             bx, by + 5, 50, 36)
    else:
        cell(_id, "", icon_style(icon_uri), bx, by, 50, 50)
    cell(_id + "_lbl", label, SMALL, bx - 20, by + 56, 90, 16)


admin_tech(0, "ad_spring", SPRING, "Spring Boot")
admin_tech(1, "ad_thy", THYMELEAF, "Thymeleaf")
admin_tech(2, "ad_htmx", None, "htmx", htmx_text=True)
admin_tech(3, "ad_alpine", ALPINE, "Alpine.js")
admin_tech(4, "ad_chart", CHARTJS, "Chart.js")
admin_tech(5, "ad_redis_cli", REDIS, "Redis client")

# Prometheus + Grafana (right of admin-app, same admin row)
PROM_X = ADMIN_APP_X + ADMIN_APP_W + 40
PROM_W = 130
GRAF_X = PROM_X + PROM_W + 30
GRAF_W = 130

cell("prom_box", "Prometheus",
     panel("#fff7ed", "#fb923c"),
     PROM_X, ADMIN_APP_Y, PROM_W, ADMIN_APP_H)
cell("prom_icon", "", icon_style(PROMETHEUS),
     PROM_X + 35, ADMIN_APP_Y + 40, 60, 60)
cell("prom_lbl", ":9090  scrape", SMALL,
     PROM_X, ADMIN_APP_Y + 105, PROM_W, 16)

cell("graf_box", "Grafana",
     panel("#fff7ed", "#fb923c"),
     GRAF_X, ADMIN_APP_Y, GRAF_W, ADMIN_APP_H)
cell("graf_icon", "", icon_style(GRAFANA),
     GRAF_X + 35, ADMIN_APP_Y + 40, 60, 60)
cell("graf_lbl", ":3000  dashboards", SMALL,
     GRAF_X, ADMIN_APP_Y + 105, GRAF_W, 16)


# ---- Data plane sub-panel
DATA_Y = ADMIN_Y + ADMIN_H + 30
DATA_H = 350  # taller to fit broker box (now 280 high) + JFR pill

cell("data_panel", "Data plane  ·  3-broker combined-mode cluster",
     panel("#ffffff", "#94a3b8", left=True),
     CL_X + 20, DATA_Y, CL_W - 40, DATA_H)

BROKER_W = 390
BROKER_H = 280  # extra room for the JFR pill below the disk pill
BROKER_Y = DATA_Y + 50
BROKER_GAP = 50

broker_xs = [
    CL_X + 50,
    CL_X + 50 + BROKER_W + BROKER_GAP,
    CL_X + 50 + 2 * (BROKER_W + BROKER_GAP),
]
titles = [
    "Broker 1  ·  voter",
    "Broker 2  ·  controller (Raft leader)",
    "Broker 3  ·  voter",
]
fills = ["#ecfdf5", "#fef3c7", "#ecfdf5"]
strokes = ["#10b981", "#f59e0b", "#10b981"]

for i, (bx, title, fill, stroke) in enumerate(zip(broker_xs, titles, fills, strokes)):
    bid = f"b{i+1}"
    cell(bid, title, panel(fill, stroke),
         bx, BROKER_Y, BROKER_W, BROKER_H)

    cell(f"{bid}_java", "", icon_style(JAVA),
         bx + 40, BROKER_Y + 50, 38, 38)
    cell(f"{bid}_java_lbl", "Java 21 · VT", SMALL,
         bx + 15, BROKER_Y + 90, 90, 16)

    cell(f"{bid}_grpc", "", icon_style(GRPC),
         bx + 140, BROKER_Y + 50, 38, 38)
    cell(f"{bid}_grpc_lbl", "gRPC · Netty", SMALL,
         bx + 115, BROKER_Y + 90, 90, 16)

    cell(f"{bid}_pb", "", icon_style(PROTOBUF),
         bx + 240, BROKER_Y + 50, 38, 38)
    cell(f"{bid}_pb_lbl", "Protobuf", SMALL,
         bx + 215, BROKER_Y + 90, 90, 16)

    cell(f"{bid}_junit", "", icon_style(JUNIT),
         bx + 330, BROKER_Y + 50, 38, 38)
    cell(f"{bid}_junit_lbl", "JUnit · jqwik", SMALL,
         bx + 305, BROKER_Y + 90, 90, 16)

    SY = BROKER_Y + 130
    pill = ("rounded=1;arcSize=18;fillColor=#ffffff;strokeColor=#94a3b8;"
            f"strokeWidth=1.5;fontColor=#0f172a;fontSize=11;fontStyle=1;{FONT};"
            "html=1;align=center;verticalAlign=middle;")
    cell(f"{bid}_raft", "raft-core", pill,
         bx + 20, SY, 110, 26)
    cell(f"{bid}_storage", "broker-storage", pill,
         bx + 140, SY, 120, 26)
    cell(f"{bid}_core", "broker-core", pill,
         bx + 270, SY, 110, 26)

    cell(f"{bid}_log_cap", "segment.log + .index + .timeindex",
         SMALL, bx + 10, SY + 32, BROKER_W - 20, 14)

    if i == 1:
        cell(f"{bid}_offsets", "+ __consumer_offsets (compacted)",
             "text;html=1;align=center;verticalAlign=middle;fontSize=11;"
             f"fontStyle=1;{FONT};fontColor=#92400e;",
             bx + 10, SY + 48, BROKER_W - 20, 16)

    cell(f"{bid}_disk", "fsync · zero-copy transferTo",
         "rounded=1;arcSize=30;fillColor=#0f172a;strokeColor=none;"
         f"fontColor=#ffffff;fontSize=10;fontStyle=1;{FONT};html=1;"
         "align=center;verticalAlign=middle;",
         bx + 100, SY + 72, 200, 26)

    # JFR pill — broker JVMs emit 6 custom events; this is where they
    # originate. Operator views the recordings in JMC (see profiling row).
    cell(f"{bid}_jfr", "JFR  ·  6 custom events",
         "rounded=1;arcSize=30;fillColor=#1e3a8a;strokeColor=none;"
         f"fontColor=#ffffff;fontSize=10;fontStyle=1;{FONT};html=1;"
         "align=center;verticalAlign=middle;",
         bx + 100, SY + 104, 200, 24)


# ---- Persistence sub-panel — narrower so it wraps the Redis content
# instead of leaving 700 px of empty space on the right.
PERS_Y = DATA_Y + DATA_H + 30
PERS_H = 110
PERS_W = 700

cell("pers_panel", "Persistence  ·  cluster-wide state (opt-in)",
     panel("#ffffff", "#94a3b8", left=True),
     CL_X + 20, PERS_Y, PERS_W, PERS_H)

# Redis box — empty value, title and body as separate left-aligned labels.
REDIS_W = 620
cell("redis_box", "",
     "rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor=#fef2f2;"
     f"strokeColor=#ef4444;strokeWidth=2;{FONT};",
     CL_X + 50, PERS_Y + 38, REDIS_W, 56)
cell("redis_icon", "", icon_style(REDIS),
     CL_X + 60, PERS_Y + 46, 40, 40)
cell("redis_title", "Redis (opt)",
     "text;html=1;align=left;verticalAlign=middle;fontSize=13;fontStyle=1;"
     f"{FONT};fontColor=#7f1d1d;",
     CL_X + 110, PERS_Y + 46, 110, 40)
cell("redis_body",
     "Quotas (RESP · INCRBY+EXPIRE)<br/>Admin event Pub/Sub fan-out",
     "text;html=1;align=left;verticalAlign=middle;fontSize=10;fontStyle=0;"
     f"{FONT};fontColor=#7f1d1d;",
     CL_X + 220, PERS_Y + 46, 440, 40)


# ---- Cluster footer — two rows: build/deploy on row 1, profiling on row 2

FOOTER_Y = PERS_Y + PERS_H + 12

# Row 1 — build & deploy (no VT-pinning gate, redundant with perf-gate)
cell("docker_logo", "", icon_style(DOCKER),
     CL_X + 30, FOOTER_Y, 28, 28)
cell("docker_text",
     "<b>Build &amp; deploy:</b>  Docker images  ·  docker compose up  ·  "
     "Helm chart for Kubernetes  ·  GitHub Actions CI  ·  perf-gate",
     "text;html=1;align=left;verticalAlign=middle;fontSize=11;fontStyle=0;"
     f"{FONT};fontColor=#1e3a8a;",
     CL_X + 70, FOOTER_Y, CL_W - 130, 28)

cell("ci_logo", "", icon_style(GH_ACTIONS),
     CL_X + CL_W - 60, FOOTER_Y, 28, 28)

# Row 2 — profiling: header + three pills + event-list caption
PROF_Y = FOOTER_Y + 38

# Header label (left side)
cell("prof_label",
     "<b>Profiling &amp; flight recording:</b>",
     "text;html=1;align=left;verticalAlign=middle;fontSize=11;fontStyle=0;"
     f"{FONT};fontColor=#1e3a8a;",
     CL_X + 30, PROF_Y, 220, 28)

# Three dark-blue pills (matching the JFR pill inside brokers — consistent
# visual treatment for runtime profiling artefacts)
def prof_pill(_id: str, x: int, label: str, w: int = 170) -> None:
    cell(_id, label,
         "rounded=1;arcSize=30;fillColor=#1e3a8a;strokeColor=none;"
         f"fontColor=#ffffff;fontSize=11;fontStyle=1;{FONT};html=1;"
         "align=center;verticalAlign=middle;",
         x, PROF_Y + 1, w, 26)

prof_pill("prof_jfr", CL_X + 250, "JFR  ·  6 custom events", w=190)
prof_pill("prof_jmc", CL_X + 450, "JMC viewer", w=130)
prof_pill("prof_asp", CL_X + 590, "async-profiler  ·  jcmd", w=190)

# Event-list caption to the right of the pills, same row
cell("prof_caption",
     "<i>Events: RaftTermChange · PartitionLeaderChange · FsyncDuration · "
     "ReplicationLag · ProduceLatency · FetchLatency</i>",
     "text;html=1;align=left;verticalAlign=middle;fontSize=10;fontStyle=0;"
     f"{FONT};fontColor=#475569;",
     CL_X + 800, PROF_Y, CL_W - 820, 28)


# ============================================================================
# ARROWS
# ============================================================================

# (1) Browser ↔ admin-app (HTTP + SSE)
edge("e1", "browser", "admin_app",
     style=BIDI_ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.4,
     label=numbered_label(1, "HTTP + SSE"))

# Each client below has a unique entry_y in the data_panel
# (height 320, top y=DATA_Y).
# Source mid-Y values: PROD=335, CONS=435, JAVA=535, CLI=635
# Target entry-Y: 0.10=DATA_Y+32, 0.34=DATA_Y+109, 0.62=DATA_Y+198, 0.90=DATA_Y+288

# (2) Producer service → data plane
edge("e2", "prod_svc", "data_panel",
     style=ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.10,
     label=numbered_label(2, "Produce · gRPC"))

# (3) Consumer service → data plane
edge("e3", "cons_svc", "data_panel",
     style=ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.34,
     label=numbered_label(3, "Fetch · Commit · gRPC"))

# (4) BrokerClient → data plane
edge("e4", "javacli_box", "data_panel",
     style=ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.62,
     label=numbered_label(4, "Idempotent produce"))

# (5) CLI → data plane
edge("e5", "cli", "data_panel",
     style=ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.90,
     label=numbered_label(5, "Admin CLI · chaos HTTP"))

# (6) Admin → data plane (single vertical at admin-app center)
edge("e6", "admin_app", "data_panel",
     style=ARROW,
     exit_x=0.5, exit_y=1,
     entry_x=0.45, entry_y=0,
     label=numbered_label(6, "Admin RPC · Metrics · Events"))

# (7) Prometheus → admin-app (horizontal scrape)
edge("e7", "prom_box", "admin_app",
     style=DASHED_ARROW,
     exit_x=0, exit_y=0.5,
     entry_x=1, entry_y=0.5,
     label=numbered_label(7, "scrape /actuator/prometheus"))

# (8) Grafana → Prometheus
edge("e8", "graf_box", "prom_box",
     style=ARROW,
     exit_x=0, exit_y=0.5,
     entry_x=1, entry_y=0.5,
     label=numbered_label(8, "PromQL"))

# (9) Data plane → Redis (vertical down)
edge("e9", "data_panel", "redis_box",
     style=ARROW,
     exit_x=0.10, exit_y=1,
     entry_x=0.20, entry_y=0,
     label=numbered_label(9, "RESP"))

# Replication arrows — short labels fit in the 60-px gaps
edge("rep_12", "b1", "b2",
     style=BIDI_ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.5,
     label="↔ replicate")

edge("rep_23", "b2", "b3",
     style=BIDI_ARROW,
     exit_x=1, exit_y=0.5,
     entry_x=0, entry_y=0.5,
     label="↔ replicate")


# ---------- assemble file --------------------------------------------------

doc = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<mxfile host="local" type="device">\n'
    '  <diagram id="architecture" name="Architecture">\n'
    f'    <mxGraphModel dx="{W}" dy="{H}" grid="0" page="1" pageScale="1" '
    f'pageWidth="{W}" pageHeight="{H}" math="0" shadow="0">\n'
    '      <root>\n'
    '        <mxCell id="0"/>\n'
    '        <mxCell id="1" parent="0"/>\n'
    f'        {chr(10).join(cells)}\n'
    '      </root>\n'
    '    </mxGraphModel>\n'
    '  </diagram>\n'
    '</mxfile>\n'
)

OUT.write_text(doc, encoding="utf-8")
print(f"wrote {OUT} ({OUT.stat().st_size} bytes, {len(cells)} cells)")

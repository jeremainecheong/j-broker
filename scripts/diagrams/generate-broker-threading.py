#!/usr/bin/env python3
"""
Generates docs/diagrams/broker-threading.drawio in AWS-reference style.

Shows the concurrency model inside a single broker JVM:
  * gRPC handler virtual threads (one VT per RPC)
  * RaftDriver pump (single VT — single source of truth for Raft state)
  * Background ticker virtual threads (controller-only vs all-brokers vs
    per-partition)
  * Shared thread-safe state
  * VT-pinning JFR gate (CI-enforced)
"""

from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
ICONS = ROOT / "docs" / "icons"
OUT = ROOT / "docs" / "diagrams" / "broker-threading.drawio"


def svg_data_uri(name: str) -> str:
    return ("data:image/svg+xml,"
            + quote((ICONS / f"{name}.svg").read_text(), safe=""))


def xml_escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


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


SMALL = (f"text;html=1;align=center;verticalAlign=middle;fontSize=10;"
         f"fontStyle=0;{FONT};fontColor=#64748b;whiteSpace=wrap;")
SMALL_LEFT = (f"text;html=1;align=left;verticalAlign=middle;fontSize=10;"
              f"fontStyle=0;{FONT};fontColor=#475569;whiteSpace=wrap;")


def icon_style(uri: str) -> str:
    return f"shape=image;html=1;imageAspect=1;image={uri};"


def vt_pill(fill: str, stroke: str, fg: str = "#0f172a") -> str:
    """Virtual-thread visualisation: rounded rectangle with bold title."""
    return (f"rounded=1;arcSize=22;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=2;fontColor={fg};fontSize=12;"
            f"fontStyle=1;{FONT};verticalAlign=middle;align=center;shadow=0;")


def state_box(fill: str, stroke: str) -> str:
    return (f"rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=1.5;fontColor=#0f172a;"
            f"fontSize=11;fontStyle=1;{FONT};verticalAlign=middle;"
            f"align=center;")


ARROW = (
    "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;"
    "html=1;labelBackgroundColor=#ffffff;labelBorderColor=#cbd5e1;"
    "exitDx=0;exitDy=0;entryDx=0;entryDy=0;"
    "endArrow=classic;endFill=1;endSize=8;"
    "strokeColor=#94a3b8;strokeWidth=1.5;"
    f"fontSize=10;fontColor=#475569;{FONT};"
)
DASHED = ARROW + "dashed=1;dashPattern=4 3;"


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
         style: str = ARROW, exit_x: float = 1, exit_y: float = 0.5,
         entry_x: float = 0, entry_y: float = 0.5) -> None:
    s = (style + f"exitX={exit_x};exitY={exit_y};"
                 f"entryX={entry_x};entryY={entry_y};")
    cells.append(
        f'<mxCell id="{_id}" value="{xml_escape(label)}" '
        f'style="{xml_escape(s)}" '
        f'edge="1" source="{source}" target="{target}" parent="1">'
        f'<mxGeometry relative="1" as="geometry"/></mxCell>'
    )


JAVA = svg_data_uri("java")
GRPC = svg_data_uri("grpc")


# ---------------------------------------------------------------------------

W, H = 1700, 1300

# JVM container
JVM_X, JVM_Y, JVM_W, JVM_H = 20, 20, W - 40, H - 40

cell("jvm",
     "Broker JVM &nbsp; · &nbsp; <b>Java 21</b> with virtual threads "
     "&nbsp; · &nbsp; ArchUnit-enforced no-pinning hot paths",
     cluster_style("#f8fafc", "#475569"),
     JVM_X, JVM_Y, JVM_W, JVM_H)

# Java logo top-right
cell("java_logo", "", icon_style(JAVA), JVM_X + JVM_W - 60, JVM_Y + 14, 36, 36)

# ---- Row 1: gRPC handler pool ---------------------------------------------

ROW1_Y = 80
ROW1_H = 270

cell("rpc_panel",
     "RPC handler virtual threads  ·  one VT per inbound RPC  ·  "
     "spawned by gRPC server  ·  nine services",
     panel("#ffffff", "#94a3b8", left=True),
     50, ROW1_Y, JVM_W - 60, ROW1_H)

# gRPC logo at the FAR RIGHT of the panel header (was overlapping the title)
cell("grpc_logo", "", icon_style(GRPC),
     50 + (JVM_W - 60) - 40, ROW1_Y + 6, 28, 28)

# Eight handler pills inside the rpc_panel (nine gRPC services — the
# TxnOffsets service rides ConsumerHandler)
HANDLERS = [
    ("h_prod",    "ProduceHandler",          "#dbeafe", "#3b82f6"),
    ("h_cons",    "ConsumerHandler",         "#dbeafe", "#3b82f6"),
    ("h_admin",   "AdminHandler",            "#dbeafe", "#3b82f6"),
    ("h_replica", "ReplicaFetchHandler",     "#dbeafe", "#3b82f6"),
    ("h_meta",    "MetadataHandler",         "#dbeafe", "#3b82f6"),
    ("h_hb",      "BrokerHeartbeatHandler",  "#dbeafe", "#3b82f6"),
    ("h_txn",     "TxnHandler",              "#dbeafe", "#3b82f6"),
    ("h_txnm",    "TxnMarkersHandler",       "#dbeafe", "#3b82f6"),
]

handler_caption = {
    "h_prod":    "Produce · InitProducerId ·<br/>quota + disk-headroom gate",
    "h_cons":    "Fetch · FindCoordinator · HB · Commit ·<br/>FetchOffsets · TxnOffsetCommit",
    "h_admin":   "CreateTopic · DeleteTopic · UpdateConfig ·<br/>ForceCompact · DeleteGroup · ResetOffsets",
    "h_replica": "ReplicaFetch ·<br/>OffsetsForLeaderEpoch",
    "h_meta":    "DescribeCluster · DescribeMetrics ·<br/>SubscribeEvents · DescribeRaft",
    "h_hb":      "BrokerHeartbeat (carries rack label)",
    "h_txn":     "InitTransactions · AddPartitionsToTxn ·<br/>AddOffsetsToTxn · EndTxn (two-phase)",
    "h_txnm":    "WriteTxnMarkers — control-batch<br/>markers from peer coordinators",
}

# Three columns of (handler pill 200 + caption 270) = 470 each
# col 0 starts at x=80, col 1 at x=560, col 2 at x=1040 — leaves comfortable
# right margin inside the rpc_panel which ends at x = 50 + JVM_W - 60.
for i, (hid, label, fill, stroke) in enumerate(HANDLERS):
    col = i % 3
    row = i // 3
    hx = 80 + col * 480
    hy = ROW1_Y + 60 + row * 70
    cell(hid, label, vt_pill(fill, stroke), hx, hy, 200, 36)
    cap = handler_caption[hid]
    cell(hid + "_cap", cap, SMALL_LEFT, hx + 210, hy, 270, 44)


# ---- Row 2: Background ticker VTs ------------------------------------------

ROW2_Y = 380
ROW2_H = 440

cell("tickers_panel",
     "Background tickers  ·  single long-lived daemon executors "
     "(plus the RaftDriver's two VTs)",
     panel("#ffffff", "#94a3b8", left=True),
     50, ROW2_Y, JVM_W - 60, ROW2_H)

# Three column groups
def ticker_group(_id: str, title: str, fill: str, stroke: str,
                 x: int, w: int, ticks: list[tuple[str, str, str]]) -> None:
    cell(_id, title, panel(fill, stroke),
         x, ROW2_Y + 50, w, ROW2_H - 70)
    for i, (tid, label, period) in enumerate(ticks):
        ty = ROW2_Y + 100 + i * 64
        cell(tid, label,
             "rounded=1;arcSize=20;whiteSpace=wrap;html=1;fillColor=#ffffff;"
             f"strokeColor={stroke};strokeWidth=1.5;fontColor=#0f172a;"
             f"fontSize=11;fontStyle=1;{FONT};verticalAlign=middle;align=center;",
             x + 18, ty, w - 36, 30)
        cell(tid + "_p", period, SMALL,
             x + 18, ty + 30, w - 36, 16)


ticker_group("ctrl_panel", "Controller-only", "#fef3c7", "#f59e0b",
             80, 480,
             [("t_fencer",  "BrokerFencer",            "every 250 ms · 3 s liveness"),
              ("t_balancer","PreferredLeaderBalancer", "every 15 s · 30 s stability")])

ticker_group("all_panel", "All brokers", "#ecfdf5", "#10b981",
             580, 480,
             [("t_house",   "Housekeeping (broker-registration)",
                                                       "every 1 s · registration · coordinator recovery · txn timeout sweep"),
              ("t_isr",     "IsrManager + ReassignmentDriver",
                                                       "every 2 s · ISR shrink/expand · reassignment steps"),
              ("t_clean",   "LogManager cleaner",      "every 5 min (default) · retention + compaction"),
              ("t_hb",      "BrokerHeartbeat sender",  "every 250 ms · point-to-point · carries rack"),
              ("t_raft",    "RaftDriver (pump + ticker)",
                                                       "1 pump VT + 1 ticker VT · 30 ms tick")])

ticker_group("perp_panel", "Per-partition / opt-in", "#dbeafe", "#3b82f6",
             1080, 480,
             [("t_replfetch","ReplicaFetcher × N",     "1 VT per partition followed"),
              ("t_txnm",     "txn-marker-delivery-* × N",
                                                       "1 VT per in-flight EndTxn marker fan-out"),
              ("t_chaos",    "ChaosHttpServer",        "small platform-thread pool · opt-in port"),
              ("t_install",  "InstallSnapshot sender", "ad-hoc · chunked snapshot to far-behind followers")])


# ---- Row 3: Shared state ---------------------------------------------------

ROW3_Y = 850
ROW3_H = 335

cell("state_panel",
     "Shared state  ·  thread-safe by construction",
     panel("#ffffff", "#94a3b8", left=True),
     50, ROW3_Y, JVM_W - 60, ROW3_H)

STATE_BOXES = [
    ("s_topic",     "TopicManager",
     "PartitionState per (topic, partition)<br/>guarded by per-partition striped locks"),
    ("s_group",     "GroupCoordinator",
     "consumer-group membership + txn offset staging<br/>ConcurrentHashMap with epoch fencing"),
    ("s_offsets",   "OffsetCache",
     "(group, topic, partition) → offset<br/>warmed from __consumer_offsets at start"),
    ("s_pidreg",    "ProducerIdRegistry",
     "(producer_id, epoch, base_seq) dedup<br/>ConcurrentHashMap with LRU cap"),
    ("s_brokers",   "BrokerRegistry + Liveness",
     "advertised host/port, last-heartbeat<br/>read by BrokerFencer"),
    ("s_log",       "LogManager + Log",
     "per-partition Log instances<br/>ReentrantLock (NOT synchronized — see VT pinning)"),
    ("s_txnc",      "TxnCoordinatorRuntime",
     "per-partition TxnCoordinator on its leader<br/>fenced by coordinatorEpoch = leader epoch"),
    ("s_quota",     "QuotaEnforcer",
     "produce/fetch rate checks at the handlers<br/>in-memory or Redis (INCRBY + EXPIRE)"),
]

for i, (sid, title, body) in enumerate(STATE_BOXES):
    col = i % 3
    row = i // 3
    sx = 80 + col * 530
    sy = ROW3_Y + 50 + row * 95
    cell(sid, title, state_box("#f0fdf4", "#22c55e"),
         sx, sy, 230, 32)
    cell(sid + "_b", body, SMALL_LEFT,
         sx + 240, sy - 4, 280, 40)


# ---- Footer: pinning gate -------------------------------------------------

FOOT_Y = ROW3_Y + ROW3_H + 25

cell("pin_logo", "🚦",
     "text;html=1;align=center;verticalAlign=middle;fontSize=22;"
     f"{FONT};fontColor=#0f172a;",
     60, FOOT_Y, 36, 36)

cell("pin_text",
     "<b>VT-pinning gate (CI-enforced):</b>  "
     "<b>VirtualThreadPinningIT</b> records <code>jdk.VirtualThreadPinned</code> JFR events "
     "during 200 concurrent produces · asserts count == 0  ·  "
     "<b>VtPinningBenchScaleIT</b> raises that to 2000 produces + 2000 fetches at bench scale  ·  "
     "fix is mechanical: <code>synchronized</code> → <code>ReentrantLock</code> on hot paths "
     "(see broker-storage/Log + LogSegment)",
     "text;html=1;align=left;verticalAlign=middle;fontSize=11;fontStyle=0;"
     f"whiteSpace=wrap;{FONT};fontColor=#1e3a8a;",
     110, FOOT_Y, JVM_W - 130, 40)


# ---- Edges --------------------------------------------------------------
# Intentionally none — the relationship is "every thread reads/writes
# every state manager safely". Drawing N×M edges would just be noise.


# ---- assemble file --------------------------------------------------------

doc = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<mxfile host="local" type="device">\n'
    '  <diagram id="broker-threading" name="Broker threading">\n'
    f'    <mxGraphModel dx="{W}" dy="{H}" grid="0" page="0" pageScale="1" '
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

#!/usr/bin/env python3
"""
Generates docs/diagrams/raft-architecture.drawio.

Shows the pure step-function design and the two paths that drive it:
  * LEFT — RaftDriver (production): real gRPC, real timers, real disk
  * CENTER — RaftCore.step: pure, deterministic, no IO/threads/clock
  * RIGHT — simulator: mock event bus, virtual clock, in-memory state,
    invariant checkers
The same RaftCore instance can be driven by either path. ArchUnit
enforces the architectural invariant that raft-core has no IO/threading
imports.
"""

from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
ICONS = ROOT / "docs" / "icons"
OUT = ROOT / "docs" / "diagrams" / "raft-architecture.drawio"


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


def big_box(fill: str, stroke: str) -> str:
    return (f"rounded=1;arcSize=6;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=3;align=center;verticalAlign=middle;"
            f"fontSize=16;fontStyle=1;{FONT};fontColor=#0f172a;shadow=0;")


def item_box(fill: str, stroke: str, fg: str = "#0f172a") -> str:
    return (f"rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor={fill};"
            f"strokeColor={stroke};strokeWidth=2;fontColor={fg};fontSize=12;"
            f"fontStyle=1;{FONT};verticalAlign=middle;align=center;")


SMALL = (f"text;html=1;align=center;verticalAlign=middle;fontSize=10;"
         f"fontStyle=0;{FONT};fontColor=#64748b;")
SMALL_LEFT = (f"text;html=1;align=left;verticalAlign=middle;fontSize=10;"
              f"fontStyle=0;{FONT};fontColor=#475569;")


def icon_style(uri: str) -> str:
    return f"shape=image;html=1;imageAspect=1;image={uri};"


ARROW = (
    "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;"
    "html=1;labelBackgroundColor=#ffffff;labelBorderColor=#cbd5e1;"
    "exitDx=0;exitDy=0;entryDx=0;entryDy=0;"
    "endArrow=classic;endFill=1;endSize=8;"
    "strokeColor=#475569;strokeWidth=2;"
    f"fontSize=11;fontColor=#0f172a;{FONT};"
)


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
JUNIT = svg_data_uri("junit")


# ---------------------------------------------------------------------------

W, H = 1700, 900

# ---- LEFT column: production driver ---------------------------------------

LEFT_X = 30
LEFT_W = 480

cell("driver_panel",
     "Production driver  ·  raft-transport",
     panel("#fffbeb", "#f59e0b", left=True),
     LEFT_X, 30, LEFT_W, 740)

# Inbound
cell("inb_box", "Inbound (gRPC server)",
     item_box("#fef3c7", "#f59e0b"),
     LEFT_X + 30, 100, 410, 50)
cell("inb_caption",
     "RaftServiceImpl deserialises wire proto<br/>"
     "→ enqueues RaftEvent on driver pump",
     SMALL_LEFT, LEFT_X + 30, 152, 410, 32)

# Pump
cell("pump_box", "RaftDriver pump  ·  single virtual thread",
     item_box("#fef3c7", "#f59e0b"),
     LEFT_X + 30, 210, 410, 50)
cell("pump_caption",
     "BlockingQueue&lt;PendingEvent&gt;<br/>"
     "drains events FIFO · single source of truth",
     SMALL_LEFT, LEFT_X + 30, 262, 410, 32)

# Ticker
cell("tick_box", "RaftDriver ticker  ·  virtual thread",
     item_box("#fef3c7", "#f59e0b"),
     LEFT_X + 30, 310, 410, 50)
cell("tick_caption",
     "every 30 ms · enqueues TickEvent<br/>"
     "drives election timeout / heartbeat cadence",
     SMALL_LEFT, LEFT_X + 30, 362, 410, 32)

# Outbound
cell("out_box", "Outbound (RaftPeerClient × N)",
     item_box("#fef3c7", "#f59e0b"),
     LEFT_X + 30, 410, 410, 50)
cell("out_caption",
     "Netty channels per peer · waitForReady before first RPC<br/>"
     "(channel.READY ≠ TCP accept — see learnings/)",
     SMALL_LEFT, LEFT_X + 30, 462, 410, 32)

# Persisted state
cell("persist_panel", "On-disk persisted state",
     panel("#fef9c3", "#ca8a04"),
     LEFT_X + 30, 540, 410, 200)

cell("frl_box", "FileRaftLog",
     item_box("#ffffff", "#94a3b8"),
     LEFT_X + 50, 580, 180, 38)
cell("frl_caption",
     "[length][CRC32][payload]<br/>"
     "torn-write recovery on start",
     SMALL_LEFT, LEFT_X + 240, 580, 190, 38)

cell("fps_box", "FilePersistentState",
     item_box("#ffffff", "#94a3b8"),
     LEFT_X + 50, 630, 180, 38)
cell("fps_caption",
     "term + votedFor (12 bytes)<br/>"
     "fsync before any vote reply",
     SMALL_LEFT, LEFT_X + 240, 630, 190, 38)

cell("snap_box", "Snapshots",
     item_box("#ffffff", "#94a3b8"),
     LEFT_X + 50, 680, 180, 38)
cell("snap_caption",
     "≥10k entries · chunked InstallSnapshot<br/>"
     "atomic rename on completion",
     SMALL_LEFT, LEFT_X + 240, 680, 190, 38)


# ---- CENTER column: RaftCore (the step function) -------------------------

CENT_X = 580
CENT_W = 540

cell("core_panel",
     "raft-core  ·  pure step-function",
     panel("#dbeafe", "#3b82f6", left=True),
     CENT_X, 30, CENT_W, 740)

# Big central step-function box
cell("step_box",
     "<b style='font-size:20px;'>RaftCore.step(RaftEvent)</b><br/>"
     "<span style='font-size:18px;'>→ List&lt;RaftEffect&gt;</span>",
     big_box("#ffffff", "#3b82f6"),
     CENT_X + 30, 130, CENT_W - 60, 130)

cell("step_caption",
     "<b>Pure</b> · no I/O, no threads, no clock<br/>"
     "<b>Deterministic</b> · same events → same effects<br/>"
     "<b>Total</b> · every event has a defined transition",
     SMALL_LEFT,
     CENT_X + 50, 280, CENT_W - 100, 60)

# DefaultRaftCore implementation contents
cell("impl_panel", "DefaultRaftCore implements every §5 rule",
     panel("#eff6ff", "#60a5fa"),
     CENT_X + 30, 360, CENT_W - 60, 410)

RULES = [
    ("r_state",   "Role + Term + log + persistent (term, votedFor)"),
    ("r_prevote", "Pre-vote (defends healthy leader from stale follower)"),
    ("r_conflict","Conflict-index fast backoff (§5.3 optimisation)"),
    ("r_commit",  "§5.4.2 commit rule — only majority-match on own term"),
    ("r_logmatch","Log-matching property — (term, index) uniquely identifies"),
    ("r_snapshot","InstallSnapshot — chunked, atomic rename"),
    ("r_xfer",    "Leadership transfer via TimeoutNow"),
]

for i, (rid, label) in enumerate(RULES):
    cell(rid, "✓  " + label,
         "rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor=#ffffff;"
         f"strokeColor=#94a3b8;strokeWidth=1.5;fontColor=#0f172a;"
         f"fontSize=11;fontStyle=0;{FONT};verticalAlign=middle;align=left;"
         "spacingLeft=14;",
         CENT_X + 50, 410 + i * 50, CENT_W - 100, 38)


# ---- RIGHT column: simulator ---------------------------------------------

RIGHT_X = 1190
RIGHT_W = 480

cell("sim_panel",
     "Simulator  ·  simulator/",
     panel("#f3e8ff", "#9333ea", left=True),
     RIGHT_X, 30, RIGHT_W, 740)

# Seeded scenario driver
cell("seed_box", "Seeded scenario driver",
     item_box("#ede9fe", "#9333ea"),
     RIGHT_X + 30, 100, 410, 50)
cell("seed_caption",
     "10 000 seeds per CI run · --seed N reproduces<br/>"
     "any failure trace exactly",
     SMALL_LEFT, RIGHT_X + 30, 152, 410, 32)

# Mock bus
cell("bus_box", "Mock event bus",
     item_box("#ede9fe", "#9333ea"),
     RIGHT_X + 30, 210, 410, 50)
cell("bus_caption",
     "in-process queue · injects message reorders /<br/>"
     "drops / partitions the real network can't",
     SMALL_LEFT, RIGHT_X + 30, 262, 410, 32)

# Virtual clock
cell("vclk_box", "Virtual clock",
     item_box("#ede9fe", "#9333ea"),
     RIGHT_X + 30, 310, 410, 50)
cell("vclk_caption",
     "advances on demand · 10k scenarios in seconds<br/>"
     "instead of hours",
     SMALL_LEFT, RIGHT_X + 30, 362, 410, 32)

# In-memory state
cell("mem_box", "In-memory log + state machine",
     item_box("#ede9fe", "#9333ea"),
     RIGHT_X + 30, 410, 410, 50)
cell("mem_caption",
     "no FileRaftLog / FilePersistentState<br/>"
     "raft-core runs identically on either backing",
     SMALL_LEFT, RIGHT_X + 30, 462, 410, 32)

# Invariant checkers
cell("inv_panel", "Safety invariants checked after every step",
     panel("#fae8ff", "#a855f7"),
     RIGHT_X + 30, 540, 410, 200)

INVARIANTS = [
    ("i_elect",   "Election safety  ·  ≤ 1 leader per term"),
    ("i_logm",    "Log-matching  ·  (term, index) → identical prior log"),
    ("i_compl",   "Leader completeness  ·  committed entries appear in every future leader"),
    ("i_apply",   "State-machine safety  ·  apply order matches commit order"),
]
for i, (iid, label) in enumerate(INVARIANTS):
    cell(iid, "✓  " + label,
         "rounded=1;arcSize=18;whiteSpace=wrap;html=1;fillColor=#ffffff;"
         f"strokeColor=#a855f7;strokeWidth=1.5;fontColor=#0f172a;"
         f"fontSize=10;fontStyle=0;{FONT};verticalAlign=middle;align=left;"
         "spacingLeft=12;",
         RIGHT_X + 50, 580 + i * 38, 370, 30)


# ---- Edges between drivers and core --------------------------------------
#
# Use bidirectional arrows on each side, anchored to the central step_box's
# left/right edges and the column panels' opposing edges. This keeps the
# labels in the gap between columns where there's empty space, so they
# don't overlap any content boxes.

BIDI = ARROW + "startArrow=classic;startFill=1;startSize=8;"

# Left column ↔ core: bidirectional, one arrow with one label
edge("e_left_io", "driver_panel", "step_box",
     style=BIDI,
     exit_x=1, exit_y=0.18, entry_x=0, entry_y=0.5,
     label="RaftEvent ↔ RaftEffect")

# Right column ↔ core: bidirectional, one arrow with one label
edge("e_right_io", "sim_panel", "step_box",
     style=BIDI,
     exit_x=0, exit_y=0.18, entry_x=1, entry_y=0.5,
     label="RaftEvent ↔ RaftEffect")


# ---- Footer: ArchUnit invariant ------------------------------------------

cell("archunit_panel",
     "<b>🔒 ArchUnit invariant (compile-time):</b>  "
     "raft-core imports zero <code>javax.*</code>, <code>jakarta.*</code>, "
     "<code>io.grpc.*</code>, <code>org.springframework.*</code>  ·  "
     "this is what makes the dual-driver design enforceable, not just aspirational",
     "rounded=1;arcSize=10;whiteSpace=wrap;html=1;fillColor=#fef9c3;"
     f"strokeColor=#eab308;strokeWidth=1.5;fontColor=#451a03;fontSize=11;"
     f"fontStyle=0;{FONT};verticalAlign=middle;align=left;spacingLeft=14;",
     30, 800, W - 60, 50)


# ---- assemble file --------------------------------------------------------

doc = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<mxfile host="local" type="device">\n'
    '  <diagram id="raft-architecture" name="Raft architecture">\n'
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

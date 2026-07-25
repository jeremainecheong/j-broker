#!/usr/bin/env bash
# Regenerate the gRPC stubs from the repo's protos into jbroker/generated/.
# The output directory is gitignored (**/generated/); run this after
# checkout and whenever proto/src/main/proto changes.
set -euo pipefail
cd "$(dirname "$0")"

PROTO_DIR="../../proto/src/main/proto"
OUT="jbroker/generated"

rm -rf "$OUT"
mkdir -p "$OUT"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    broker.proto common.proto

# protoc emits absolute imports (import common_pb2 ...); rewrite them to
# package-relative so the stubs live inside jbroker.generated.
python - <<'EOF'
import pathlib
import re

out = pathlib.Path("jbroker/generated")
for f in out.glob("*_pb2*.py"):
    text = f.read_text()
    text = re.sub(r"^import (\w+_pb2) as", r"from . import \1 as", text, flags=re.M)
    f.write_text(text)
(out / "__init__.py").write_text(
    '"""Generated gRPC stubs. Run clients/python/generate.sh to regenerate."""\n'
)
EOF

echo "generated stubs in clients/python/$OUT"

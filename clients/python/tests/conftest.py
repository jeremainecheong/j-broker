"""Shared fixtures: boot a real broker from the installDist binary.

The E2E tests skip cleanly when the binary is absent; build it with

    ./gradlew :broker-app:installDist
"""

from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_BIN = _REPO_ROOT / "broker-app" / "build" / "install" / "broker-app" / "bin" / "broker-app"
_BOOT_TIMEOUT_S = 90.0
_READY_TIMEOUT_S = 30.0


def broker_binary() -> Path | None:
    """The broker launcher, from $JBROKER_BIN or the installDist default."""
    override = os.environ.get("JBROKER_BIN")
    path = Path(override) if override else _DEFAULT_BIN
    return path if path.is_file() else None


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class BrokerProcess:
    process: subprocess.Popen
    host: str
    port: int
    binary: Path
    output: list[str]

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def tail(self, n: int = 40) -> str:
        return "\n".join(self.output[-n:])


def _start_once(binary: Path, data_dir: Path) -> BrokerProcess | None:
    """One boot attempt on fresh ports. None if the process died early
    (port-bind race) — the caller retries with new ports."""
    broker_port = _free_port()
    raft_port = _free_port()
    process = subprocess.Popen(
        [
            str(binary),
            "server",
            "--id",
            "1",
            "--data-dir",
            str(data_dir),
            "--broker-port",
            str(broker_port),
            "--raft-port",
            str(raft_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output: list[str] = []
    listening = threading.Event()

    def pump():
        for line in process.stdout:
            output.append(line.rstrip("\n"))
            if "j-broker listening on" in line:
                listening.set()

    threading.Thread(target=pump, daemon=True).start()

    deadline = time.monotonic() + _BOOT_TIMEOUT_S
    while time.monotonic() < deadline:
        if listening.is_set():
            return BrokerProcess(process, "127.0.0.1", broker_port, binary, output)
        if process.poll() is not None:
            return None  # died before listening — likely a port collision
        time.sleep(0.1)
    process.kill()
    raise TimeoutError(f"broker did not report listening within {_BOOT_TIMEOUT_S}s:\n" + "\n".join(output))


def _wait_until_ready(broker: BrokerProcess) -> None:
    """Block until the single-voter controller has elected itself —
    DescribeCluster reports controller_id == 1."""
    import grpc

    from jbroker._stubs import broker_pb2, broker_pb2_grpc

    with grpc.insecure_channel(broker.addr) as channel:
        stub = broker_pb2_grpc.MetadataStub(channel)
        deadline = time.monotonic() + _READY_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                response = stub.DescribeCluster(broker_pb2.DescribeClusterRequest(), timeout=2.0)
                if response.error == 0 and response.controller_id == 1:
                    return
            except grpc.RpcError:
                pass
            time.sleep(0.2)
    raise TimeoutError(f"controller not elected within {_READY_TIMEOUT_S}s; broker output:\n{broker.tail()}")


@pytest.fixture(scope="session")
def broker(tmp_path_factory) -> BrokerProcess:
    binary = broker_binary()
    if binary is None:
        pytest.skip("broker binary not found — run ./gradlew :broker-app:installDist (or set JBROKER_BIN)")
    started = None
    for attempt in range(3):
        data_dir = tmp_path_factory.mktemp(f"jbroker-data-{attempt}")
        started = _start_once(binary, data_dir)
        if started is not None:
            break
    if started is None:
        pytest.fail("broker exited during startup on 3 attempts (port-bind race?)")
    try:
        _wait_until_ready(started)
        yield started
    finally:
        started.process.terminate()
        try:
            started.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            started.process.kill()
            started.process.wait(timeout=5)

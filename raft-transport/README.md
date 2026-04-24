# raft-transport

The gRPC shell around `raft-core`. Responsible for turning the pure step-function into a live cluster: hosting the Raft peer RPC server, dialing peers, and running the event loop that feeds `RaftCore.step` with timer / inbound-RPC / apply-response events.

## Key types

| Type | Purpose |
|---|---|
| `RaftDriver` | The event loop. Pulls from a bounded queue, calls `RaftCore.step`, dispatches emitted `RaftEffect`s (send RPC, reset timer, persist state). Single virtual thread — no shared mutable state with outside callers. |
| `RaftPeerClient` | Outbound gRPC client for `AppendEntries`, `RequestVote`, `TimeoutNow`, `InstallSnapshot`. |
| `RaftServiceImpl` | Inbound gRPC service. Translates wire proto into `RaftEvent`s the driver enqueues. |
| `TlsConfig`, `TlsContexts` | P15.2 mTLS bundle. Optional — brokers without TLS configured get a plain Netty channel. |

## Why separate from raft-core

The same `RaftCore` instance is driven by both `RaftDriver` (production) and the deterministic simulator. Extracting I/O into `raft-transport` means the core has no dependency on gRPC, no thread affinity, no time source — every non-determinism gets injected as an event.

## Architectural invariants (ArchUnit)

- `raft-transport` can import `raft-core` but `raft-core` cannot import `raft-transport`.
- No Spring dependency — brokers wire it by hand in `broker-app`.

## Testing

- ~10 tests for the event-loop plumbing: RPC deserialisation, effect dispatch, queue back-pressure.
- Broker-app ITs run real 3-node clusters against this transport; see `integration-tests/README.md`.

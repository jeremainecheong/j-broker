# Contributing

j-broker is a single-maintainer project. Issues and pull requests are welcome; review focuses on correctness and test quality over surface area. For larger changes, open an issue first — the boundaries in the README's [What this is not](README.md#what-this-is-not) section are settled.

## Building

Prerequisites:

- Java 21 (Temurin). Always use the wrapper (`./gradlew`) — it is pinned to Gradle 8.7 and SHA-256 verified.
- Docker, for the Testcontainers-backed tests and the compose cluster.

```bash
./gradlew build                          # compile + unit tests + fast integration tests
./gradlew :integration-tests:stressTest  # 100 randomised election cycles
JBROKER_RUN_SLOW_TESTS=1 ./gradlew test  # slow suite: 1M-record compaction, 10k clients, Redis IT
```

## The testing bar

Changes land with tests at the layer that can catch their bugs: unit tests for logic, integration tests on a real 3-node loopback cluster for wiring, simulator seeds for consensus behaviour. The layer map is in the README under [Verification culture](README.md#verification-culture).

One rule is absolute: **"transient" is not a diagnosis.** A failing CI check is never rerun until green — it gets root-caused and fixed, because every "flake" found in this tree so far has been a real bug. A PR with an intermittently failing test is not done.

## Pull requests

- CI must be fully green: build, perf gates, proto wire-compatibility, and the secured Helm install on Kind.
- Keep commits small and focused — one logical change per commit, conventional subjects (`feat:`, `fix:`, `test:`, `docs:`).
- Proto changes must be additive; the `buf` job fails PRs that break wire compatibility against the base branch.
- Fill in the PR template: what changed, why, and exactly how you verified it.

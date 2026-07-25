# Security policy

## Reporting a vulnerability

Report vulnerabilities privately via [GitHub security advisories](https://github.com/jeremainecheong/j-broker/security/advisories/new). Do not open a public issue for anything exploitable. Reports get an acknowledgement within a week; fix timelines depend on severity.

## Supported versions

| Version | Supported |
|---|---|
| `main` | Yes — security fixes land here |
| Tagged releases | None exist yet — the project is pre-1.0, with no versioned release |

Until the first `v*` release, `main` is the only supported line.

## Scope

The hardened configuration — mTLS, ACL default-deny, admin login, chaos gate — is described in the README's [Security](README.md#security) section. A plaintext dev cluster on an open network is a misconfiguration, not a vulnerability; reports should assume the hardened configuration.

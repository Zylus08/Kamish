# PR #1 review fixes — 7 November 2026

The review was performed on PR head `91a4c22c418566a8f0ae6fc76b245e3ab0a32c74`.
This revision integrates existing main `bacc9372ba3a87f7b123409f7bcb53a4958632b7`
without replacing its newer CLI, authentication, package launchers, or seeded demo content.

## Corrections

- Reject path traversal, encoded separators/dot segments, and symlink escapes in gateway static files and the unified CLI's model fallback. Missing static files return 404.
- Preserve main's framed TCP listener and run proactive ownership renewal for the unified service; stop both on shutdown. Report its actual local in-memory authority.
- Bound live telemetry to one refresh per five seconds and coalesce concurrent polling/SSE requests. Reconnect event subscriptions after login.
- Use authenticated CLI requests and retrieve the independent session CSRF token before mutations. Align topic request/response fields with the gateway.
- Verify signed policy publication against the configured trusted issuer, policy identity, freshness, and monotonic version before installing it. Load init-provisioned policy trust at unified startup. Explicit floor-only requests remain supported.
- Resolve the dashboard action to a real catalog topic/class and pass the selected local/global residency restriction to the broker. Unsupported dashboard policies require the explicit console instead of invented class names.

## Fresh local verification

- `cargo test --workspace --offline`: **251 passed**, zero failures.
- Includes a real unified subprocess test that writes over framed TCP, waits **31 seconds**, writes again, and confirms active leases via authenticated HTTP.
- CLI integration verifies topic creation/list/status, trusted signed policy installation, and rejection of an untrusted issuer.
- Static-file tests exercise unauthenticated traversal and canonical/symlink containment.
- `cargo test --manifest-path early_research/probes/Cargo.toml --offline`: **17 passed**.
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- Frontend tests, `npm run typecheck`, and production build: passed. The build retains the existing large-chunk warning.
- TypeScript SDK tests/build: **23 tests passed**.

Socket-based tests ran with local networking permission after sandbox restrictions prevented binding.
No external etcd cluster, KMS, independent witness service, production browser deployment,
registry publication, or cross-platform native runtime was verified in this pass. Policy
publication updates the running cache; persistent multi-region policy rollout remains outside
this local-runtime verification. Historical 261-test evidence retains its original source scope.

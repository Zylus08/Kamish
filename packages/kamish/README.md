# kamish

Unified command-line interface, sovereign event streaming broker, and embedded operations dashboard for Kamish.

## Overview

`kamish` is the single global entry point for developing, operating, and auditing sovereign event streaming topologies. It embeds:
- The regional sovereign broker daemon (sub-millisecond local ingestion, append-only WAL, outbox isolation).
- The REST/SSE API gateway.
- The web management dashboard with real-time audit chain verification and telemetry.

## Installation

```bash
npm install -g kamish
```

When installed via npm, native binaries are resolved via platform-specific `optionalDependencies` with zero post-install download scripts.

## Quickstart

### 1. Initialize Cluster Skeleton & Policy Keys
```bash
kamish init
```
Generates configuration (`kamish.toml`), provisions an initial administrator account, generates Ed25519 signing keys (`policy.key`, `policy.pub`), and authors a default residency policy (`policy.json`).

### 2. Verify System Prerequisites
```bash
kamish doctor
```
Diagnoses system readiness, port availability, consensus reachability, and configuration validity with actionable remediation advice.

### 3. Launch Unified Broker, Gateway & Dashboard
```bash
kamish start --dev
```
*(Or use the developer alias: `kamish run --dev`)*

Spins up the broker daemon, REST/SSE gateway, and embedded web dashboard in a single orchestrated process at `http://127.0.0.1:3001`.

## Subcommands

- `kamish start`: Launch broker, gateway, and dashboard in one process.
- `kamish run`: Developer alias for `kamish start` (e.g. `kamish run --dev`).
- `kamish doctor`: Validate ports, versions, consensus reachability, and configuration.
- `kamish init`: Author configuration skeletons, Ed25519 signing keys, and genesis policy.
- `kamish status`: Query cluster state, authority consensus model, active leases, and quorum status.
- `kamish topic create|list`: Create and inspect sovereign topics.
- `kamish policy sign|verify|publish`: Author, cryptographically verify, and publish Ed25519 policies.
- `kamish produce`: Produce records with residency policy enforcement.
- `kamish consume|fetch`: Read records from partition offsets.
- `kamish verify-audit`: Verify cryptographic hash-chain integrity of audit logs.
- `kamish benchmark`: Run ingestion and calibrated network impairment benchmarks.
- `kamish demo`: Execute scenario walkthrough with explicit REAL vs SIMULATED badges.

## Limitations & Honesty Disclosures

- **Consensus & Multi-Region**: Single-node mode runs without `etcd` using local fsync storage (`LOCAL_PERSISTENT_STORAGE`). Multi-region CAS ownership, lease fencing, and partition promotion require an active `etcd` cluster.
- **Experimental Alpha**: This is release `0.1.0-alpha.1` intended for local evaluation and research validation. Production use requires independent cryptographic and operational audit.
- **What We Do NOT Claim**:
  - We do NOT claim zero-loss without quorum-backed replication.
  - We do NOT claim drop-in Kafka protocol wire equivalence.
  - We do NOT claim hardware-level enclave sealing for unencrypted local disk logs.

## License

Apache-2.0


# HeltesDB

This project was built as a test of [Claude Code](https://claude.ai/claude-code), Anthropic's CLI coding agent.

HeltesDB is a distributed, sharded key-value store implementing Snapshot Isolation (SI) via a two-phase commit protocol, verified against a TLA+ specification.

## Architecture

- **Coordinator** — client-facing gRPC service that manages transaction lifecycle (Begin/Read/Update/Commit/Abort) and drives 2PC across shards
- **Shard** — stores MVCC version history per key; handles Read, Update, Prepare, Commit, Abort from coordinators
- **Consistent hashing** — FNV-1a 64-bit ring with 150 virtual nodes per shard routes keys to shards
- **TLA+ spec** (`spec/HeltesDB.tla`) — models the full SI protocol; verified with TLC (708k states)

## Scaling

- **Horizontal shard scaling** — add shards; consistent hashing redistributes keys with minimal disruption
- **Horizontal coordinator scaling** — each coordinator is independent; the coordinator's identity is encoded in the high 32 bits of every `tx_id`, so shards and coordinators can route cross-coordinator Inquire RPCs without a registry

## Benchmarking

A Go benchmark under `benchmarking/` hammers the coordinator with concurrent Begin→Update→Commit transactions using dynamic gRPC (no code generation needed).

```bash
cd benchmarking
go run . -addr '[::1]:50052' -workers=200 -keyspace=10000 -dur=30s
```

Typical single-shard numbers on a laptop: ~3000–3500 committed tx/s at p50 ~15ms.

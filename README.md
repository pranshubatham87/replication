# Production Distributed Replication Engineering (FastAPI + SQLite)

An end-to-end, six-project engineering repository demonstrating how **Single-Leader**, **Multi-Leader**, **Leaderless Quorum**, **Causal Version Vectors**, **Conflict-Free Replicated Data Types (CRDTs)**, and **Replication Log Internals & Lag Mitigation** work in production systems—and how to handle the failure modes they introduce.

## Modules
1. `module_01_single_leader_user_profiles` — Single-Leader Replicated User-Profile Service (FastAPI + SQLite WAL, sync/semi-sync/async acks, lag metrics, consistent snapshot catch-up, read-your-writes, monotonic reads, epoch-fenced leader failover).
2. `module_02_multi_leader_collab_workspace` — Multi-Region Collaborative Workspace & Offline Sync (`us-east`, `eu-west`, `ap-south` leaders, Star/Circular/All-to-All topologies with loop prevention, sync/async conflict detection, home-leader conflict avoidance, HLC LWW & custom field mergers).
3. `module_03_leaderless_quorum_kv_store` — Dynamo-Style Leaderless Quorum KV Store (5-node consistent-hash ring, tunable N/W/R quorums, stale read detection, synchronous inline Read Repair, SHA-256 Merkle Anti-Entropy sync, Sloppy Quorum & Hinted Handoff).
4. `module_04_concurrency_version_vectors` — Concurrency & Causal Version Vector Engine (happens-before vs. concurrent sibling detection, simulated NTP clock skew proving naive LWW silent data loss, timestamp collision tie-breaking, and 3-way shopping-cart/field merge).
5. `module_05_crdts_convergent_state` — Convergence & CRDT Service (State-based CvRDT and Op-based CmRDT SQLite replicas for G-Counter, PN-Counter, 2P-Set, and OR-Set with Add-Wins semantics and join-semilattice property verification under duplicate/out-of-order delivery).
6. `module_06_replication_internals_lag_mitigation` — Replication Internals & Lag Mitigation Pipeline (Statement-Based replication non-deterministic divergence vs. SQLite Native Trigger CDC Outbox & Physical WAL row images, plus Read-Your-Writes, Monotonic Reads, and Consistent-Prefix Reads for causal social feeds).

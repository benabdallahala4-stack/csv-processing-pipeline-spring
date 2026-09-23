# ADR 006: Defer cache and replicas

**Status:** Accepted

The first release uses PostgreSQL as the authoritative status store. A cache adds invalidation paths, and a read replica can return stale state immediately after completion. Add either only after measurements show a status-read bottleneck and the API defines acceptable staleness.

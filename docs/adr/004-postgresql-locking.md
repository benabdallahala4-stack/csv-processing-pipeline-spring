# ADR 004: PostgreSQL locking

**Status:** Accepted

Use `READ COMMITTED`, uniqueness constraints, `SELECT FOR UPDATE` for one upload, and `FOR UPDATE SKIP LOCKED` for publisher batches. Locks are short and never surround network calls.

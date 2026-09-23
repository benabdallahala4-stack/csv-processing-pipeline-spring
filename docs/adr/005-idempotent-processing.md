# ADR 005: Idempotent processing

**Status:** Accepted

Deduplicate HTTP commands by request hash, Kafka events by inbox event ID, and partner submissions by upload ID. These stable identities allow safe recovery across every at-least-once boundary.

# ADR 003: Transactional outbox

**Status:** Accepted

Write the READY state and `CsvUploadReady` event in one PostgreSQL transaction. A leased publisher sends rows to Kafka. This closes the dual-write gap while accepting harmless duplicate publication after an ambiguous crash.

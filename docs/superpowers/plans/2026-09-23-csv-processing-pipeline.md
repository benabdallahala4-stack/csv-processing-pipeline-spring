# CSV Processing Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-shaped Spring Boot pipeline that uploads CSV files directly to AWS S3 and reliably submits them to a third-party service through PostgreSQL outbox and Kafka processing.

**Architecture:** A Maven multi-module project keeps pure domain and application ports independent of Spring. Independently runnable API, worker, and third-party stub applications wire PostgreSQL, AWS S3, Kafka, and HTTP adapters. PostgreSQL `READ COMMITTED`, explicit row locks, unique constraints, leases, an outbox, and an inbox provide recoverable at-least-once processing with effectively-once business outcomes.

**Tech Stack:** Java 21, Spring Boot 4.1.1, Maven, Spring JDBC, PostgreSQL 17, Flyway, Spring Kafka, AWS SDK for Java 2.x S3, Testcontainers, LocalStack test service, JUnit 5, AssertJ, Awaitility, WireMock, ArchUnit, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-23-csv-processing-pipeline-design.md`

## Global Constraints

- Production storage is AWS S3 exclusively.
- Domain and application modules contain no Spring, JDBC, Kafka, HTTP, or AWS imports.
- Database isolation is PostgreSQL `READ COMMITTED`; long network operations never run inside database transactions.
- HTTP, Kafka, and third-party boundaries are idempotent and safe under duplicate delivery.
- All commits use only `Ala Ben Abdallah <benabdallahala4@gmail.com>` with no additional author trailers.
- README Mermaid must use GitHub-supported syntax and render successfully.

---

### Task 1: Build skeleton and enforce clean boundaries

**Files:**
- Create: `pom.xml`, `.mvn/wrapper/*`, `mvnw`, `mvnw.cmd`
- Create: `modules/upload-domain/pom.xml`, `modules/upload-application/pom.xml`, `modules/upload-adapters/pom.xml`
- Create: `applications/upload-api/pom.xml`, `applications/processing-worker/pom.xml`, `applications/third-party-stub/pom.xml`
- Test: `applications/upload-api/src/test/java/com/benabdallah/csvpipeline/ArchitectureTest.java`

**Interfaces:**
- Produces Maven coordinates under `com.benabdallah.csvpipeline` and dependency direction `applications -> adapters -> application -> domain`.

- [ ] Write an ArchUnit test that imports `com.benabdallah.csvpipeline..` and asserts domain and application packages do not depend on Spring, AWS, Kafka, JDBC, servlet, or adapter packages.
- [ ] Run `./mvnw test` and confirm the initial build fails because modules and application entry points do not exist.
- [ ] Add the parent and module POMs, Java 21 compiler configuration, dependency management, Maven Wrapper, and minimal application entry points.
- [ ] Run `./mvnw test` and confirm all modules compile and the architecture test passes.
- [ ] Commit with `build: create clean architecture module skeleton`.

### Task 2: Model upload state and retry behavior

**Files:**
- Create: `modules/upload-domain/src/main/java/com/benabdallah/csvpipeline/upload/Upload.java`
- Create: `modules/upload-domain/src/main/java/com/benabdallah/csvpipeline/upload/UploadId.java`
- Create: `modules/upload-domain/src/main/java/com/benabdallah/csvpipeline/upload/UploadStatus.java`
- Create: `modules/upload-domain/src/main/java/com/benabdallah/csvpipeline/upload/UploadFailure.java`
- Create: `modules/upload-domain/src/main/java/com/benabdallah/csvpipeline/upload/RetryPolicy.java`
- Test: `modules/upload-domain/src/test/java/com/benabdallah/csvpipeline/upload/UploadTest.java`
- Test: `modules/upload-domain/src/test/java/com/benabdallah/csvpipeline/upload/RetryPolicyTest.java`

**Interfaces:**
- Produces `Upload.initiate(...)`, `markReady()`, `claimProcessing(Instant)`, `scheduleRetry(...)`, `succeed(...)`, and `fail(...)`.
- Produces `RetryPolicy.nextDelay(int attempt, UUID jitterSeed): Duration`.

- [ ] Write parameterized tests for every permitted and rejected state transition, including repeated `markReady()` behavior.
- [ ] Run domain tests and verify they fail because the model is missing.
- [ ] Implement immutable identifiers, explicit state-transition methods, stable failure codes, and deterministic exponential backoff with bounded jitter.
- [ ] Run domain tests and verify all transitions and retry calculations pass.
- [ ] Commit with `feat: model CSV upload lifecycle`.

### Task 3: Define application use cases and ports

**Files:**
- Create: `modules/upload-application/src/main/java/com/benabdallah/csvpipeline/application/port/in/CreateUploadUseCase.java`
- Create: `modules/upload-application/src/main/java/com/benabdallah/csvpipeline/application/port/in/CompleteUploadUseCase.java`
- Create: `modules/upload-application/src/main/java/com/benabdallah/csvpipeline/application/port/in/GetUploadUseCase.java`
- Create: `modules/upload-application/src/main/java/com/benabdallah/csvpipeline/application/port/in/ProcessUploadUseCase.java`
- Create: outbound ports under `application/port/out` for uploads, idempotency, outbox, inbox, object storage, event publication, and third-party submission
- Create: services under `application/service`
- Test: service unit tests under `modules/upload-application/src/test/java`

**Interfaces:**
- `CreateUploadUseCase.create(CreateUploadCommand): CreateUploadResult`
- `CompleteUploadUseCase.complete(UploadId): UploadView`
- `GetUploadUseCase.get(UploadId): UploadView`
- `ProcessUploadUseCase.process(CsvUploadReadyEvent): ProcessingResult`
- `ObjectStoragePort.presignPut(...)`, `head(...)`, and `openStream(...)`
- `TransactionRunner.required(Supplier<T>): T` makes transaction intent explicit without Spring imports.

- [ ] Write mock-port tests for identical and conflicting HTTP idempotency keys, repeat completion, object mismatch, duplicate events, retryable third-party failures, and permanent CSV failures.
- [ ] Run application tests and verify failures name missing use cases.
- [ ] Implement commands, results, ports, and orchestration services with network calls outside `TransactionRunner` callbacks.
- [ ] Run application tests and verify orchestration and call ordering pass.
- [ ] Commit with `feat: add upload application use cases`.

### Task 4: Create PostgreSQL schema and locking adapters

**Files:**
- Create: `modules/upload-adapters/src/main/resources/db/migration/V1__create_pipeline_schema.sql`
- Create: JDBC adapters under `modules/upload-adapters/src/main/java/com/benabdallah/csvpipeline/adapter/postgres`
- Test: `modules/upload-adapters/src/test/java/com/benabdallah/csvpipeline/adapter/postgres/PostgresConcurrencyTest.java`

**Interfaces:**
- Implements upload, idempotency, outbox, inbox, and transaction ports using `JdbcClient`, `TransactionTemplate`, and PostgreSQL.
- Adds `findByIdForUpdate` using `SELECT ... FOR UPDATE`.
- Adds `claimOutboxBatch` using a CTE with `FOR UPDATE SKIP LOCKED` and a lease deadline.

- [ ] Write Testcontainers tests proving unique idempotency, one logical ready event under concurrent completion, exclusive outbox claims, and expired-lease recovery.
- [ ] Run adapter tests and verify they fail before the migration and adapters exist.
- [ ] Add Flyway DDL with checks, foreign keys, documented partial indexes, and JDBC mappings; implement `READ COMMITTED` transactions and explicit locks.
- [ ] Run adapter tests and inspect PostgreSQL assertions for duplicates, row states, and claim ownership.
- [ ] Commit with `feat: add PostgreSQL consistency adapters`.

### Task 5: Add AWS S3 upload and streaming adapter

**Files:**
- Create: `modules/upload-adapters/src/main/java/com/benabdallah/csvpipeline/adapter/s3/S3ObjectStorageAdapter.java`
- Create: `modules/upload-adapters/src/main/java/com/benabdallah/csvpipeline/adapter/s3/S3Properties.java`
- Test: `modules/upload-adapters/src/test/java/com/benabdallah/csvpipeline/adapter/s3/S3ObjectStorageAdapterTest.java`

**Interfaces:**
- Implements `ObjectStoragePort` using `S3Client` and `S3Presigner`.
- Object keys are generated server-side as `uploads/{uploadId}/source.csv`.
- `head` verifies length, content type, and `sha256` metadata; `openStream` returns a closeable stream.

- [ ] Write LocalStack Testcontainers tests for presigned PUT, metadata verification, missing objects, and streaming download.
- [ ] Run the S3 adapter test and verify it fails because the adapter is missing.
- [ ] Implement the AWS SDK adapter with path-style access configurable only for tests and local endpoints.
- [ ] Run S3 tests and verify the application never buffers the complete object.
- [ ] Commit with `feat: integrate direct AWS S3 uploads`.

### Task 6: Expose the REST API

**Files:**
- Create: API controllers, request/response records, problem handler, security-safe correlation filter, and Spring configuration under `applications/upload-api/src/main/java`
- Create: `applications/upload-api/src/main/resources/application.yml`
- Test: controller and full integration tests under `applications/upload-api/src/test/java`

**Interfaces:**
- `POST /api/v1/uploads`
- `POST /api/v1/uploads/{uploadId}/complete`
- `GET /api/v1/uploads/{uploadId}`
- Errors use `application/problem+json`.

- [ ] Write MockMvc contract tests for validation, required `Idempotency-Key`, replay, conflict, completion, not found, and safe problem details.
- [ ] Run API tests and verify endpoints return 404 before controllers exist.
- [ ] Implement thin controllers, DTO mapping, RFC 9457 error handling, correlation IDs, and explicit wiring.
- [ ] Run API tests and verify exact status codes, headers, and JSON bodies.
- [ ] Commit with `feat: expose idempotent upload API`.

### Task 7: Publish the transactional outbox to Kafka

**Files:**
- Create: `modules/upload-adapters/src/main/java/com/benabdallah/csvpipeline/adapter/kafka/OutboxPublisher.java`
- Create: event serializer and topic configuration in the Kafka adapter package
- Create: scheduling and executor configuration in `applications/upload-api`
- Test: `modules/upload-adapters/src/test/java/com/benabdallah/csvpipeline/adapter/kafka/OutboxPublisherTest.java`

**Interfaces:**
- Publishes versioned `CsvUploadReady` JSON keyed by upload ID.
- Marks publication only after Kafka acknowledges; failures release or expire the lease with backoff.

- [ ] Write tests for successful publish, broker failure, concurrent publishers, and crash-window duplicate publication.
- [ ] Run publisher tests and verify failure before implementation.
- [ ] Implement bounded polling, lease ownership, Kafka acknowledgment handling, retry scheduling, and metrics.
- [ ] Run tests against PostgreSQL and Kafka containers and prove duplicates retain one event ID.
- [ ] Commit with `feat: publish upload events through outbox`.

### Task 8: Process Kafka events and call the third party

**Files:**
- Create: Kafka listener, CSV streaming validator, HTTP third-party adapter, retry scheduler, and worker configuration under adapters and `applications/processing-worker`
- Create: idempotent endpoint and persistence in `applications/third-party-stub`
- Test: worker unit and integration tests under `applications/processing-worker/src/test/java`

**Interfaces:**
- Consumes `csv-upload-ready.v1` with manual acknowledgment.
- Calls `POST /api/v1/imports` with `Idempotency-Key: {uploadId}` and streams validated rows.
- Produces `SUCCEEDED`, `RETRY_PENDING`, or `FAILED` durable outcomes.

- [ ] Write tests for duplicate Kafka records, invalid CSV, timeout after third-party acceptance, 429/5xx retry, permanent 4xx failure, expired worker lease, and exhausted attempts.
- [ ] Run worker tests and verify failure before listener and adapters exist.
- [ ] Implement bounded streaming validation, recoverable inbox leases, stable third-party idempotency, manual Kafka acknowledgment, retry scheduling, and dead-letter handling.
- [ ] Run integration tests and verify one stub import exists after duplicate and ambiguous-timeout scenarios.
- [ ] Commit with `feat: process uploads with recoverable Kafka worker`.

### Task 9: Add operations, Compose, diagrams, ADRs, and end-to-end verification

**Files:**
- Create: `compose.yaml`, Dockerfiles for each application, `.env.example`
- Create: `README.md`
- Create: six ADRs under `docs/adr`
- Create: `scripts/smoke-test.ps1` and `scripts/smoke-test.sh`
- Create: CI workflow under `.github/workflows/ci.yml`

**Interfaces:**
- Compose starts PostgreSQL, Kafka, API, worker, and third-party stub; S3 settings come from environment variables.
- README documents AWS S3 setup, curl upload flow, row locks, `READ COMMITTED`, outbox/inbox failure windows, and metrics.

- [ ] Write the smoke test first so it creates an upload, sends a file to its presigned S3 URL, completes it, polls status, and requires `SUCCEEDED`.
- [ ] Add Actuator health groups, Micrometer metrics, JSON log correlation, graceful shutdown, Docker images, Compose health checks, and CI.
- [ ] Write GitHub-compatible Mermaid component, sequence, state, database, concurrency, and failure-recovery diagrams; render-check every Mermaid block.
- [ ] Add ADRs for hexagonal modules, direct S3 upload, transactional outbox, isolation and locks, inbox/idempotency, and deferring cache/read replicas.
- [ ] Run `./mvnw clean verify`, architecture tests, integration tests, Compose health checks, and the smoke test; record commands and expected results in README.
- [ ] Scan tracked files and all commits for prohibited attribution and unwanted naming, then commit with `docs: add runnable architecture guide`.

### Task 10: Publish and verify the GitHub repository

**Files:**
- No product files added unless remote verification finds a documentation defect.

**Interfaces:**
- Produces private GitHub repository `benabdallahala4-stack/csv-processing-pipeline-spring` with `main` as default branch.

- [ ] Re-run the full verification suite from a clean build.
- [ ] Verify every commit author and committer is exactly `Ala Ben Abdallah <benabdallahala4@gmail.com>` and no co-author trailers exist.
- [ ] Create the private repository under the authenticated `benabdallahala4-stack` account and push `main`.
- [ ] Verify GitHub Actions and inspect the rendered README diagrams on GitHub.
- [ ] Fix and repush any remote-only rendering or CI issue, then report the repository URL and verification evidence.

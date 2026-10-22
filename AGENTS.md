# AGENTS.md - Quarkus Outbox Extension

## Project Context
High-performance, database-agnostic Transactional Outbox extension for Quarkus.
Uses virtual bucketing to allow safe active-active multi-pod Kafka dispatching while preserving strict FIFO ordering per aggregate ID.

- **Baseline**: Java 21 LTS
- **Build Tool**: Apache Maven (>= 3.9.x)
- **Parent**: Inherits from `io.quarkiverse:quarkiverse-parent`
- **Maven Coordinates**: Group: `io.github.thoweber`, Parent Artifact: `quarkus-outbox-kafka-parent`
- **Java Root Package**: `io.github.thoweber.quarkus.outbox.kafka.*`
- **Core Dependencies**: Quarkus 3.15+, Hibernate ORM (`quarkus-hibernate-orm`), Apache Kafka Client (`quarkus-kafka-client`)

---

## Module Layout & Boundary Rules

1. `runtime/`
    - Contains CDI beans, JPA entities (`OutboxEvent`, `OutboxLock`), lock providers, and Kafka dispatchers.
    - **Rule**: Never import `io.quarkus.deployment.*` classes in runtime.

2. `deployment/`
    - Contains build-time processors and `@BuildStep` methods.
    - Responsible for registering beans (`AdditionalBeanBuildItem`), indexing JPA entities (`AdditionalJpaModelBuildItem`), and generating synthetic bytecode.
    - **Rule**: Do not put runtime business execution logic here.
    - **Testing Rule**: Build-time unit tests in `deployment/src/test` test extension augmentation logic; do not run broker containers here. Keep `quarkus.kafka.devservices.enabled=false` in deployment unit tests.

3. `integration-tests/`
    - Integration tests executing against real Quarkus Dev Services (PostgreSQL container + Kafka container).
    - Validates multi-pod concurrency, lease failover, and native compilation.
    - Dev Services are enabled here to provide real Testcontainers environments.

---

## CI/CD Pipeline & Pull Request Workflows

The repository uses GitHub Actions (`.github/workflows/ci.yaml`) with a two-stage pipeline:
1. **JVM Build & Test (`build`)**:
   - Executes formatting validation (`./mvnw process-sources && git diff --exit-code`) and JVM verification (`./mvnw clean verify`).
   - Runs automatically on all pushes and pull requests targeting `main` and `master`.
2. **Native Integration Tests (`native`)**:
   - Compiles and runs native image tests using Mandrel 21 (`./mvnw verify -pl integration-tests -Dnative`).
   - **Trunk (`main`/`master`)**: Runs automatically upon completion of the JVM build.
   - **Pull Requests**: Gated behind a mandatory manual review stage via the GitHub Environment `native-integration-tests`.
   - **Auto-Invalidation**: Concurrency is configured on the PR branch with `cancel-in-progress: true`, automatically canceling and invalidating previous approvals whenever a new commit is pushed to the PR branch.

---

## Critical Build & Verification Commands

Always run these commands before submitting changes or finishing a task:

- **Format Code**:
  ```bash
  ./mvnw process-sources
  ```
  Note: CI strictly enforces Quarkus code formatting and import ordering inherited from `quarkiverse-parent`. If `git diff --exit-code` fails after compile, formatting was missed.

- **Fast Compile & Unit Tests:**
  ```bash
  ./mvnw test-compile
  ```

- **Full Verification (includes Dev Services container tests):**
  ```bash
  ./mvnw clean verify
  ```
- **Run Single Integration Test:**
  ```bash
  ./mvnw verify -pl integration-tests -Dit.test=OutboxWorkerIT
  ```
- **GraalVM Native Build Validation:**
   ```bash
   ./mvnw verify -pl integration-tests -Dnative
   ```

---

## Architectural Guardrails & Invariants

When writing or modifying code in this repository, strictly adhere to these architectural rules:

### 1. Database Transactions (The Short-Transaction Sandwich)

* NEVER hold an open database connection or transaction across external network calls (such as Kafka broker dispatches).
* Always use the 3-phase lifecycle:
  1. Short Read: Fetch a batch of eligible records and immediately commit/close the transaction.
  2. Network Send: Dispatch records to Kafka and collect successful/failed message acknowledgments in memory.
  3. Short Write: Issue a single batch delete (or retry update) for acknowledged IDs in a separate transaction.

### 2. Transaction Boundaries & CDI Proxies
* Do not call `@Transactional` methods on `this` within the same class; self-invocation bypasses ArC client proxies.
* Use `QuarkusTransaction.requiringNew().call(...)` or `QuarkusTransaction.requiringNew().run(...)` for programmatic transaction control.
* Alternatively, delegate database operations to an injected separate repository bean.

### 3. Concurrency, Locking & Fail-Fast Timing
* Virtual bucket locks ($M$, default 1024) must be acquired using atomic compare-and-swap (CAS) updates against pre-seeded rows in `OutboxLock`.
* Dedicated Bulkhead Pool: Lock CAS updates and heartbeats must execute against the dedicated Agroal DataSource `quarkus.datasource."outbox-lock"`, physically isolated from application connection pools.
* Background heartbeat supervisor loops renewing bucket leases must execute on Java 21 virtual threads (`Thread.ofVirtual()`) at interval $T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$.
* When lease renewal fails past $\frac{2}{3} \times T_{\text{lease}}$, the supervisor must set `leaseLost = true` and call `workerThread.interrupt()`.
* Cooperative Worker Buffer: Workers must check `if (leaseTimeRemaining < Duration.ofSeconds(2))` before each record dispatch, aborting active batches before lease expiration.

### 4. Identifiers, Generation Gating & Database Portability
* **Persistent UUIDv7:** Entity primary key `id` must be generated as a time-ordered UUIDv7 (RFC 9562) and mapped 1:1 to CloudEvents `ce_id`. Dynamic random UUID generation in dispatch loops is forbidden.
* **Online Generation Gating:** Zero-downtime resharding uses a persistent `generation` column with index `(generation ASC, bucket_id ASC, next_retry_at ASC, id ASC)` and a worker poller barrier (`SELECT MIN(generation)`).
* **Portability:** Stick to standard ANSI JPQL and standard JPA mappings (`Instant`, `@Lob` or large text). Do not introduce vendor-specific native SQL queries in default beans.

### 5. Management & Operator REST API
* Administrative endpoints must be mounted under the non-application path `/q/outbox-kafka/admin`:
  - `GET /q/outbox-kafka/admin/quarantined` (list quarantined aggregates)
  - `GET /q/outbox-kafka/admin/quarantined/{id}/events` (view dead letter + trailing events)
  - `POST /q/outbox-kafka/admin/quarantined/{id}/replay` (replay aggregate)
  - `POST /q/outbox-kafka/admin/quarantined/{id}/skip` (mark as SKIPPED and unfreeze)
  - `DELETE /q/outbox-kafka/admin/quarantined/{id}` (purge pending events)
  - `POST /q/outbox-kafka/admin/drain` (gracefully drain pending bucket events)

### 6. Performance, GC & Memory Invariants (Measurable SLAs)
* **Allocation Rate Budget:** Code in worker and dispatch loops must strictly adhere to $\le \mathbf{512\text{ bytes}}$ heap allocation per dispatched record.
* **Zero JPA Entity Hydration:** NEVER query or hydrate full `OutboxEvent` managed entities in polling loops. Always use Hibernate `StatelessSession` with direct constructor record projections (`OutboxBatchRecord`).
* **Header & Buffer Zero-Copy:** Use `static final byte[]` constants for CloudEvents header keys. Avoid intermediate byte copying or defensive `String` allocations.
* **Throughput & Latency:** Target single-pod throughput $\ge \mathbf{10{,}000}\text{ msgs/sec}$; transit latency $\text{p95} \le \mathbf{50\text{ms}}$; DB connection hold time strictly $\mathbf{0\text{ms}}$ during Kafka transit.
* **Native Image Memory:** Extension steady-state heap overhead must remain $\le \mathbf{32\text{ MB}}$; GraalVM Native Image container RSS must remain $\le \mathbf{64\text{ MB}}$ idle and $\le \mathbf{96\text{ MB}}$ under peak throughput.

### 7. Summary Matrix of Invariants for Coding Agents

| Subsystem | Required Pattern | Prohibited Anti-Pattern |
| :--- | :--- | :--- |
| **Identifier** | Persisted UUIDv7 (`id`) mapped to CloudEvents `ce_id` | Auto-incrementing sequences or in-memory `UUID.randomUUID()` |
| **Persistence** | Short-Transaction Sandwich (Read $\to$ I/O $\to$ Delete) | Holding `@Transactional` or DB connection across Kafka network sends |
| **Hydration** | Direct Record projection via `StatelessSession` | Hydrating managed JPA entities in polling loops |
| **Allocations** | Low allocation ($\le 512\text{B}$/event, static header bytes) | Dynamic string formatting / JSON object allocations in dispatch loop |
| **Locking** | Pre-seeded rows ($0 \dots M-1$) + JPQL CAS updates | Vendor-specific `INSERT ... ON CONFLICT` or dynamic upserts |
| **DataSource** | Dedicated bulkhead named pool (`outbox-lock`) | Sharing application connection pool for heartbeats |
| **Dispatch** | Synchronous/pipelined per-bucket FIFO wait | Fire-and-forget asynchronous batch dispatch |
| **Virtual Sharding**| Online Generation Gating (`generation` poller barrier) | Dynamically changing bucket modulo without epoch drain |
| **Error Handling** | Aggregate Quarantining (`next_retry_at = 9999`) | Dropping poison pills while letting subsequent aggregate events flow |
| **Concurrency** | Virtual threads for supervisors & heartbeats | Unbounded platform thread pools or blocking Vert.x Event Loop |
| **Lease Timing** | Fail-fast at $\frac{2}{3} \times T_{\text{lease}}$ & worker cooperative $2\text{s}$ buffer | Proceeding with network send when lease is on the verge of expiration |

---

## Mandatory Integration Test Suites

Refer to [`integration-tests/TEST_GUIDE.md`](file:///home/developer/projects/quarkus-outbox-kafka/integration-tests/TEST_GUIDE.md) and [`.agents/rules/02-outbox-invariants.md`](file:///home/developer/projects/quarkus-outbox-kafka/.agents/rules/02-outbox-invariants.md) for complete details on:
1. `PhantomProducerIT` (STW GC pause simulation, lease steal, late flush deduplication via persistent UUIDv7 `ce_id`)
2. `ShortTransactionSandwichIT` (Zero active DB connections during Kafka transit)
3. `AggregateQuarantineIT` (Poison pill dead-lettering, trailing aggregate freeze, sibling aggregate drainage, admin replay/skip)
4. `OutboxConcurrencyIT` (Multi-pod contention and per-aggregate monotonic FIFO arrival)
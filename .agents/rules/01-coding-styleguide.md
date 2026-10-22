# Engineering & Coding Style Guide: Quarkus Transactional Outbox

## 1. Governance, Packaging & Quarkiverse Standards

All contributions to `quarkus-tx-outbox` must adhere to Quarkus extension architecture and Quarkiverse Hub publishing conventions.

### 1.1 Module Separation & Dependency Invariants
The codebase is partitioned into four distinct modules. Maintain strict separation of concerns across module boundaries:

```
quarkus-outbox/
├── runtime/              # Execution logic, CDI beans, JPA entities, Kafka dispatchers
├── deployment/           # Build steps (@BuildStep), bytecode generation, Jandex indexing
├── integration-tests/    # Dev Services tests (JVM & Native image)
└── docs/                 # Antora documentation modules
```

* **Runtime Module (`runtime/`):**
  * **Invariant:** Never import any package from `io.quarkus.deployment.*`.
  * Dependencies must remain lean. Do not transitively introduce testing or deployment dependencies.
* **Deployment Module (`deployment/`):**
  * **Invariant:** No runtime execution logic or stateful singletons may reside here.
  * All runtime CDI beans must be explicitly registered via `AdditionalBeanBuildItem` (marked `.setUnremovable()` when dynamic lookup or SPI resolution is used).
  * Entities must be registered with Hibernate ORM using `AdditionalJpaModelBuildItem` so consumer applications do not need manual entity scanning or `persistence.xml` files.
* **Integration Tests Module (`integration-tests/`):**
  * Tests must execute in both JVM mode (`./mvnw clean verify`) and GraalVM native image mode (`./mvnw verify -Dnative`).
* **Documentation Module (`docs/`):**
  * Documentation is authored in AsciiDoc using Antora directory layouts (`modules/ROOT/pages/`) for automated publishing to `docs.quarkiverse.io`.

### 1.2 Maven Coordinates & Parent POM
* **Parent POM:** Root `pom.xml` must inherit from `io.quarkiverse:quarkiverse-parent`.
* **Personal vs. Quarkiverse Namespaces:**
  * When incubating under personal GitHub accounts (e.g., `io.github.<user>`), suppress the inherited Maven Enforcer check in the root `pom.xml`:
    ```xml
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-enforcer-plugin</artifactId>
        <executions>
            <execution>
                <id>validate-project-structure</id>
                <phase>none</phase>
            </execution>
        </executions>
    </plugin>
    ```
  * Upon official transfer to Quarkiverse, group coordinates flip to `io.quarkiverse.outbox` and the enforcer override is removed.
* **Artifact Naming Standard:** Follow the Quarkus artifact naming conventions:
  * Aggregator: `quarkus-outbox-parent`
  * Runtime: `quarkus-outbox`
  * Deployment: `quarkus-outbox-deployment`
  * Integration Tests: `quarkus-outbox-integration-tests`

### 1.3 Formatting, Linting & License Headers
* **Automated Formatting:** Source code formatting and import ordering are enforced by `impsort-maven-plugin` and `formatter-maven-plugin` as inherited from the Quarkiverse parent.
* **Format Execution:** Always execute the format phase before staging commits:
  ```bash
  ./mvnw process-sources
  ```
* **CI Diff Check:** The continuous integration pipeline validates source hygiene using:
  ```bash
  ./mvnw -B process-sources && git diff --exit-code
  ```
* **License Headers:** Every `.java` file must start with the standard Apache License 2.0 header.

---

## 2. Java 21 LTS Platform & Idiomatic Usage

The hard compilation baseline is **Java 21 LTS** (`<maven.compiler.release>21</maven.compiler.release>`).

### 2.1 Virtual Threads
* Background bucket heartbeats, periodic supervisor loops, and non-blocking scheduling tasks should leverage Java 21 virtual threads:
  ```java
  Thread.ofVirtual()
        .name("outbox-heartbeat-bucket-" + bucketId)
        .start(heartbeatRunnable);
  ```
* Do not allocate unbounded traditional OS platform thread pools for I/O waiting routines.

### 2.2 Immutability & Data Transfer Records
* All batch snapshots, internal queue items, and event envelopes must be modeled as immutable `record` types:
  ```java
  public record OutboxBatchRecord(
      long id,
      String aggregateId,
      String aggregateType,
      int bucketId,
      String destinationTopic,
      String payload,
      Instant createdAt
  ) {}
  ```
* Deconstruct data using Record Patterns and Pattern Matching instead of deep getter chains:
  ```java
  if (record instanceof OutboxBatchRecord(long id, var aggId, var type, int bId, var topic, var payload, var ts)) {
      // Direct variable binding
  }
  ```

### 2.3 Sequenced Collections
* Access boundary records in batch fetches via Sequenced Collection methods (`getFirst()`, `getLast()`) rather than manual index calculations (`list.get(0)`, `list.get(list.size() - 1)`):
  ```java
  long firstId = batch.getFirst().id();
  long lastId = batch.getLast().id();
  ```

### 2.4 Language Guardrails
* Do not enable JVM preview features (`--enable-preview`) in production builds. The extension must run seamlessly on stock Java 21 LTS distributions (Temurin, Mandrel, GraalVM).

---

## 3. Persistence, Transactions & Database Portability

The outbox persistence engine must operate **100% database-agnostically** across PostgreSQL, MySQL, MariaDB, Oracle, Microsoft SQL Server, and H2 without relying on vendor-specific SQL dialects.

### 3.1 The "Short-Transaction Sandwich" Invariant

> **Rule:** Never hold a database transaction or connection across network operations (such as Kafka broker sends).

Every bucket drain operation must adhere strictly to three distinct phases:

```
[Poller Thread]
   │
   ├── Phase 1: Short Read TX (StatelessSession)
   │     - Fetch pending batch (e.g., 50 items)
   │     - Commit TX and release connection back to Agroal pool immediately
   │
   ├── Phase 2: Network Send Phase (Kafka Producer)
   │     - Dispatch records over the wire to Kafka brokers
   │     - Await broker ACKs (or reactive pipeline completions)
   │     - 0 Database connections held during this phase
   │
   └── Phase 3: Short Write TX (StatelessSession Bulk Delete)
         - Open fresh short transaction
         - Execute single JPQL DELETE for all acknowledged event IDs
         - Commit TX and return connection
```

### 3.2 Transaction Demarcation & CDI Proxies
* **No Self-Invocation:** Calling `@Transactional` methods directly on `this` within the same bean bypasses ArC interceptor proxies, resulting in silent omission of transactions.
* **Programmatic Demarcation:** Prefer programmatic boundaries using `QuarkusTransaction`:
  ```java
  List<OutboxEvent> batch = QuarkusTransaction.requiringNew().call(() -> {
      return session.createQuery(...).getResultList();
  });
  ```
* **Dedicated Repositories:** When using declarative `@Transactional(TxType.REQUIRES_NEW)`, place queries in separate injected repository beans to ensure calls transit the ArC client proxy.

### 3.3 Persistence Optimization via `StatelessSession`
* Use Hibernate's `org.hibernate.StatelessSession` for outbox polling, batch reading, and bulk deletions.
* **Why:** `StatelessSession` avoids 1st-level session cache overhead, completely eliminates dirty checking, bypasses entity state cascades, and executes bulk updates via `createMutationQuery()`, achieving raw JDBC performance while retaining type-safe JPA metadata.

### 3.4 Cross-Database Entity Modeling Rules
* **Primary Keys:** Use `GenerationType.IDENTITY` or database sequences with `allocationSize = 1` to ensure compatibility across MySQL, PostgreSQL, and Oracle without dialect discrepancies.
* **Payload Storage:** Store message payloads in a `@Lob` annotated `String` field. Hibernate automatically handles the native mapping:
  * PostgreSQL / MySQL: `TEXT`
  * Oracle: `CLOB`
  * SQL Server: `VARCHAR(MAX)`
  * *Never use proprietary column types such as `JSONB` in core entities.*
* **Timestamps:** Store all temporal points (`createdAt`, `nextRetryAt`, `expiresAt`) as `java.time.Instant`.

### 3.5 Pre-Seeded Lock Entities (Zero Upsert Requirement)
* Standard JPQL lacks cross-database `UPSERT` (`INSERT ... ON CONFLICT DO UPDATE` or `MERGE`).
* All virtual lock rows (from `bucket_id = 0` to `M - 1`) **must be pre-seeded** in the `outbox_lock` table during schema initialization.
* Lock acquisition and lease extension must execute as a clean, atomic Compare-And-Swap (CAS) JPQL query:
  ```java
  int updated = session.createMutationQuery(
      "UPDATE OutboxLock l " +
      "SET l.lockedBy = :podId, l.expiresAt = :newExpiry " +
      "WHERE l.bucketId = :bucketId AND (l.expiresAt < :now OR l.lockedBy = :podId)")
      .setParameter("podId", podId)
      .setParameter("newExpiry", newExpiry)
      .setParameter("bucketId", bucketId)
      .setParameter("now", now)
      .executeUpdate();
  ```

### 3.6 Connection Pool Bulkheading
* Configure an isolated named DataSource (e.g., `quarkus.datasource."outbox-lock".*`) for lock acquisition and heartbeat management.
* This bulkhead ensures that application connection pool saturation cannot starve background lock heartbeats and cause split-brain lease expirations.

---

## 4. Concurrency, Locking & Polling Architecture

### 4.1 Virtual Bucketing
* Virtual bucketing decouples table partitioning from physical worker threads while preserving strict per-aggregate chronological ordering.
* Assign bucket IDs deterministically:
  $$bucket\_id = \left\vert{} \text{hashCode}(aggregate\_id) \right\vert{} \pmod M$$
* Set a fixed, large virtual shard count ($M = 1024$) in the database to prevent resharding migrations when Kafka partition counts increase.

### 4.2 Pluggable Lock Provider SPI (`OutboxLockProvider`)
* All locking mechanics must implement the `OutboxLockProvider` interface:
  ```java
  public interface OutboxLockProvider {
      boolean tryLock(int bucketId, Duration leaseTime);
      void renewHeartbeat(int bucketId, Duration leaseTime);
      void unlock(int bucketId);
  }
  ```
* Provide a default table-based JPA implementation (`TableBasedLockProvider`), while allowing applications to supply vendor implementations (such as PostgreSQL Advisory Locks or Redis/Redisson) via CDI alternatives or `@LookupIfProperty`.

### 4.3 Fail-Fast Heartbeat Supervision
* Active bucket workers must renew leases asynchronously at regular intervals ($T_{\text{interval}} = \frac{T_{\text{lease}}}{3}$).
* If a heartbeat fails to renew before reaching $\frac{2}{3} \times T_{\text{lease}}$, it must:
  1. Flag an internal volatile variable: `leaseLost = true`.
  2. Interrupt the active worker thread.
* The dispatch loop must inspect `leaseLost` prior to dispatching each message, aborting execution immediately to prevent stale dual-dispatch (split-brain) conditions.

### 4.4 Non-Blocking & Reactive Thread Safety
* **Event Loop Protection:** Never execute blocking database transactions or synchronous `.await()` calls on Vert.x Event Loop threads.
* Defensively assert thread safety in blocking entry points:
  ```java
  if (!io.quarkus.runtime.BlockingOperationControl.isBlockingAllowed()) {
      throw new IllegalStateException("Outbox blocking dispatch cannot be called on an Event Loop thread. Offload to a worker pool or use the reactive API.");
  }
  ```

---

## 5. Kafka Dispatching & CloudEvents Standards

### 5.1 Native `KafkaProducer` Client
* Depend directly on `io.quarkus:quarkus-kafka-client` rather than static SmallRye Reactive Messaging channels (`@Channel`).
* **Why:** The outbox requires runtime destination topic routing, access to Kafka's `RecordAccumulator`, low-allocation batch processing, and direct broker ACK callbacks.

### 5.2 CNCF CloudEvents Binary Mode Headers
* Every record dispatched to Kafka must include CloudEvents v1.0 binary mode headers:
  * `ce_id`: Unique identifier (String representation of the outbox event ID or UUID).
  * `ce_type`: Aggregate domain event type.
  * `ce_source`: Originating service URI / identifier.
  * `ce_time`: ISO-8601 creation timestamp.
  * `content-type`: Payload mime type (e.g., `application/json`).
  * `x-outbox-bucket`: Virtual bucket ID (diagnostic metadata).
* Downstream consumers rely on `ce_id` as an immutable deduplication key to absorb duplicate deliveries caused by network retries.

### 5.3 Partition Key Alignment
* Set `aggregateId` as the Kafka record key (`ProducerRecord(topic, aggregateId, payload)`).
* Kafka's default partitioner hashes the record key, ensuring all events for an aggregate land on the same Kafka partition in identical FIFO order.

---

## 6. Fault Tolerance, Dead-Lettering & Quarantining

### 6.1 Exponential Backoff & Local Bucket Halting
* When a message dispatch fails (e.g., Kafka broker timeout):
  1. Increment `retry_count`.
  2. Compute next retry time:
     $$\text{delaySeconds} = \text{initialBackoff} \times (\text{multiplier}^{(\text{retryCount} - 1)})$$
  3. Update `next_retry_at` in an isolated short transaction.
  4. Immediately halt processing for that bucket to maintain FIFO sequence.

### 6.2 Aggregate Quarantining Protocol
* Upon exceeding `max_retries`:
  1. Move the failed event into `outbox_dead_letter` and delete it from `outbox_event`.
  2. **Quarantine Invariant:** Freeze subsequent events belonging to the same aggregate to prevent out-of-order execution:
     ```sql
     UPDATE outbox_event 
     SET next_retry_at = '9999-12-31 23:59:59Z' 
     WHERE aggregate_id = :failedAggregateId
     ```
  3. The remainder of the virtual bucket continues processing unimpeded.

### 6.3 Operator Admin API (`/q/outbox-admin`)
The extension must expose secured administrative endpoints for operational support:
* `GET /q/outbox-admin/quarantined`: List all quarantined aggregate IDs.
* `GET /q/outbox-admin/quarantined/{id}/events`: Inspect the dead-lettered event and pending tail.
* `POST /q/outbox-admin/quarantined/{id}/replay`: Reset `next_retry_at = now()` to retry.
* `POST /q/outbox-admin/quarantined/{id}/skip`: Mark dead event as `SKIPPED` and unfreeze pending events.
* `POST /q/outbox-admin/quarantined/{id}/purge`: Delete all pending records for the aggregate.

---

## 7. Testing, Dev Services & Native Validation

### 7.1 Zero Mocking of External Infrastructure
* Never mock databases or Kafka brokers in integration tests.
* Rely on Quarkus Dev Services (`quarkus-jdbc-postgresql`, `quarkus-kafka-client`) to spin up real PostgreSQL and Kafka Testcontainers automatically.

### 7.2 Active-Active Concurrency Verification
Integration tests must simulate multi-pod contention by executing multiple worker instances against the shared database:
1. Verify that no two workers hold the same bucket lock simultaneously.
2. Verify that per-aggregate FIFO order is strictly preserved under high concurrency.
3. Verify that zombie workers are safely rejected upon lease expiration.

### 7.3 GraalVM Native Compilation Validation
Run native tests prior to preparing releases:
```bash
./mvnw verify -pl integration-tests -Dnative
```
Ensure all entity classes, records, and serializers are indexed in `OutboxProcessor` without generating reflection errors during native image construction.
# Architecture Specification: Quarkus Transactional Outbox (`quarkus-outbox-kafka`)

## 1. Executive Summary & Purpose

The **Quarkus Transactional Outbox** extension provides an enterprise-grade, active-active, horizontally scalable Transactional Outbox implementation for Quarkus applications streaming events to Apache Kafka. 

It eliminates the operational overhead of Change Data Capture (CDC / Debezium / Kafka Connect pipelines) while avoiding the throughput bottlenecks and race conditions of naive single-pod polling solutions.

### Core Value Proposition
- **Multi-Pod Active-Active Scaling:** Virtual bucketing allows $K$ pod replicas to drain outbox tables concurrently without inter-pod locks or collisions.
- **Strict FIFO Ordering per Aggregate:** Events mapped to the same aggregate ID are guaranteed to be dispatched to Kafka in strict chronological sequence.
- **Database-Agnostic via JPA:** Built on Jakarta Persistence and Hibernate ORM (`StatelessSession`), supporting PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, and H2 without vendor-specific SQL dialects.
- **Kafka-Native Dispatch:** Direct integration with Kafka Producer semantics, pipelined batching, and CloudEvents v1.0 binary mode metadata.
- **Workload-Aware Fairness:** Dynamic bin-packing dynamically schedules worker capacity toward congested shards while starving neither low-traffic nor noisy tenants.

---

## 2. Platform & Toolchain Baseline

| Attribute | Specification |
| :--- | :--- |
| **Java Platform** | Java 21 LTS (`<maven.compiler.release>21</maven.compiler.release>`) |
| **Quarkus Stream** | Quarkus 3.15.1+ / Quarkus 4.x |
| **Build Tool** | Apache Maven 3.9+ |
| **Parent POM** | `io.quarkiverse:quarkiverse-parent:21` |
| **Target Coordinates** | Group: `io.github.thoweber` (incubating) / `io.quarkiverse.outboxkafka`, Artifact: `quarkus-outbox-kafka-parent` |
| **Java Root Package** | `io.github.thoweber.quarkus.outbox.kafka.*` |
| **Persistence Engine** | Hibernate ORM 6.x (`quarkus-hibernate-orm`), `StatelessSession` |
| **Messaging Client** | Quarkus Kafka Client (`quarkus-kafka-client`) |

### 2.1. Measurable Performance & Resource SLAs

The extension enforces strict, measurable operational service levels:

| Category | Metric / SLA | Target Threshold | Validation Tool |
| :--- | :--- | :--- | :--- |
| **Throughput** | Single-Pod Batch Drain Throughput | $\ge \mathbf{10{,}000}\text{ msgs/sec}$ (batch 100, 1KB payload, PostgreSQL + Kafka) | Micrometer / JMH |
| **Latency** | End-to-End Transit Latency (DB commit $\to$ Kafka ACK) | **p50:** $\le \mathbf{15\text{ms}}$, **p95:** $\le \mathbf{50\text{ms}}$, **p99:** $\le \mathbf{100\text{ms}}$ | Micrometer Timer |
| **DB Hold Time**| Connection Borrow Duration in Phase 2 (Kafka transit) | $\mathbf{0\text{ms}}$ (hard zero active DB connections held during I/O) | Agroal Active Count |
| **Backlog Scan**| Backlog Estimation Scan Duration (`idx_outbox_gen_bucket_fifo`) | **p99:** $\le \mathbf{2\text{ms}}$ (composite index boundary probe) | Poller Execution Timer |
| **GC Allocation**| Heap Allocation Rate per Dispatched Record | $\le \mathbf{512\text{ bytes}}$ / record (excluding Kafka accumulator) | JFR / Async-profiler |
| **Hydration** | Entity Hydration Allocations in Dispatch Loop | **0 managed entity objects** (exclusively use `StatelessSession` Records) | JFR Allocation Profile |
| **GC Pause** | Maximum Stop-The-World (STW) Pause Duration | **p99.9:** $\le \mathbf{10\text{ms}}$ (ZGC / Shenandoah), **Max STW:** $< \mathbf{50\text{ms}}$ (G1) | GC Logs / JVM Metrics |
| **JVM Heap** | Steady-State Extension Heap Overhead | $\le \mathbf{32\text{ MB}}$ retained heap memory | JConsole / Live Heap |
| **Native RSS** | GraalVM Container Resident Set Size (`VmRSS`) | $\le \mathbf{64\text{ MB}}$ baseline; $\le \mathbf{96\text{ MB}}$ under peak 10k msgs/sec | Linux `VmRSS` |
| **Stack Size** | Virtual Thread Stack Overhead per Supervisor Loop | $\le \mathbf{1\text{ KB}}$ per virtual thread stack | Virtual Thread Dump |
| **In-Flight Mem**| Bounded Uncommitted Batch Buffer | $\le \mathbf{5\text{ MB}}$ per pod ($C_{\text{pod}} = 500 \times \text{batchSize}$) | Bounded Queue Metric |
| **Ordering** | Monotonic FIFO Arrivals per Aggregate ID | **Strictly 0 out-of-order anomalies** ($100\%$ monotonic arrival) | `OutboxConcurrencyIT` |
| **Deduplication**| Replay Deduplication Efficiency via UUIDv7 `ce_id` | $\mathbf{100\%}$ duplicate identification on failover | `PhantomProducerIT` |
| **Isolation** | Bulkhead Heartbeat Success under Primary Pool Exhaustion | $\mathbf{100\%}$ heartbeat success within $\frac{1}{3} \times T_{\text{lease}}$ deadline | `ShortTransactionSandwichIT` |

---

## 3. Strict Architectural Guardrails & Invariants

All agents and contributors modifying this codebase must adhere to the following invariants:

### 3.1. The "Short-Transaction Sandwich"
> **Rule:** Never hold a database transaction or connection across network operations (such as Kafka broker dispatches).

Every polling execution must adhere to a strict 3-phase lifecycle:
1. **Short Read Transaction:** Execute a short, read-only transaction to fetch an eligible batch of records. Commit and release the DB connection immediately.
2. **Network I/O Phase:** Dispatch records over the network to Kafka. Wait for broker ACKs (or reactive pipeline completions). No DB connection may be open during this time.
3. **Short Write Transaction:** Open a second, independent transaction to execute a bulk delete (or retry status update) for all successfully acknowledged event IDs in a single statement.

```
[Worker Thread]
   │
   ├── Phase 1: BEGIN TX -> Read Batch (StatelessSession) -> COMMIT TX (DB connection returned)
   │
   ├── Phase 2: Kafka Produce -> Await Broker ACK (NO DB CONNECTION HELD)
   │
   └── Phase 3: BEGIN TX -> Bulk Delete Acknowledged IDs -> COMMIT TX
```

### 3.2. Transaction Isolation & CDI Proxy Traversal
- **Self-Invocation Forbidden:** Never call a `@Transactional` method directly from another method within the same bean class; ArC interceptor proxies are bypassed on self-invocation.
- **Programmatic Control Preferred:** Use `QuarkusTransaction.requiringNew().call(...)` or `QuarkusTransaction.requiringNew().run(...)` to guarantee short, isolated transaction lifecycles.
- **StatelessSession First:** The polling engine must utilize `org.hibernate.StatelessSession` rather than `EntityManager` to eliminate first-level persistence cache overhead, prevent unintended dirty checking, and avoid heap ballooning during bulk reads.

### 3.3. Isolation of Distributed Locks and Heartbeats
- **Dedicated Bulkhead Connection Pool:** Lock acquisition, CAS updates, and background heartbeats must execute against an isolated Agroal named data source (`quarkus.datasource."outbox-lock"`). This physically bulkheads lock heartbeats from application connection pool starvation caused by domain transaction spikes or slow queries (ADR-005).
- **Virtual Thread Supervision:** Background bucket heartbeats must execute on Java 21 virtual threads (`Thread.ofVirtual()`), renewing the active bucket lease at fixed intervals:
  $$T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$$
- **Fail-Fast Boundary Invariant:** If database connection latency or stalls prevent lease renewal past $\frac{2}{3} \times T_{\text{lease}}$:
  1. The supervisor immediately sets `volatile boolean leaseLost = true`.
  2. The supervisor calls `workerThread.interrupt()`.
  3. The worker must check `if (leaseLost)` prior to every network dispatch and abort the active batch before the lease expires on the database tier, preventing split-brain dual dispatch.

### 3.4. Cooperative Fail-Fast Deadline Buffer & TOCTOU Defense
- **Worker Cooperative Deadline Buffer:** Before initiating network transmission of each record, workers verify that the active bucket lease has at least a 2000ms safety buffer remaining:
  ```java
  if (leaseTimeRemaining.compareTo(Duration.ofSeconds(2)) < 0) {
      log.warnf("Lease safety deadline (< 2000ms) reached for bucket %d. Yielding batch to prevent split-brain race.", bucketId);
      break;
  }
  ```
- **Persistent UUIDv7 Message Identification:** Every outbox record must carry an immutable, time-ordered UUIDv7 (RFC 9562) as its primary key. The CloudEvents binary header `ce_id` is mapped directly and immutably from `event.id.toString()`. Generating dynamic identifiers in memory on dispatch (e.g., `UUID.randomUUID()`) is strictly prohibited.
- **Consumer Deduplication Contract:** The extension operates under an at-least-once delivery contract. Downstream consumers evaluate incoming `ce_id` values against an idempotency store to silently drop replays resulting from network retries or late socket buffer flushes.

### 3.5. Summary Matrix of Architectural Invariants

| Subsystem | Required Pattern | Prohibited Anti-Pattern |
| :--- | :--- | :--- |
| **Identifier** | Persisted UUIDv7 (`id`) mapped to CloudEvents `ce_id` | Auto-incrementing sequences or in-memory `UUID.randomUUID()` |
| **Persistence** | Short-Transaction Sandwich (Read $\to$ I/O $\to$ Delete) | Holding `@Transactional` or DB connection across Kafka network sends |
| **Locking** | Pre-seeded rows ($0 \dots M-1$) + JPQL CAS updates | Vendor-specific `INSERT ... ON CONFLICT` or dynamic upserts |
| **DataSource** | Dedicated bulkhead named pool (`outbox-lock`) | Sharing application connection pool for heartbeats |
| **Dispatch** | Synchronous/pipelined per-bucket FIFO wait | Fire-and-forget asynchronous batch dispatch |
| **Virtual Sharding**| Online Generation Gating (`generation` poller barrier) | Dynamically changing bucket modulo without epoch drain |
| **Error Handling** | Aggregate Quarantining (`next_retry_at = 9999`) | Dropping poison pills while letting subsequent aggregate events flow |
| **Concurrency** | Virtual threads for supervisors & heartbeats | Unbounded platform thread pools or blocking Vert.x Event Loop |
| **Lease Timing** | Fail-fast at $\frac{2}{3} \times T_{\text{lease}}$ & worker cooperative $2\text{s}$ buffer | Proceeding with network send when lease is on the verge of expiration |

---

## 4. Domain Entities & Database Schema

The persistence model consists of three core tables managed via JPA.

### 4.1. Entity: `OutboxEvent` (`outbox_event`)
Stores pending events created within the business transaction.

```java
@Entity
@Table(
    name = "outbox_event",
    indexes = {
        @Index(name = "idx_outbox_gen_bucket_fifo", columnList = "generation ASC, bucket_id ASC, next_retry_at ASC, id ASC"),
        @Index(name = "idx_outbox_aggregate", columnList = "aggregate_id")
    }
)
public class OutboxEvent {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    public UUID id; // Generated via UUIDv7 (RFC 9562 time-ordered UUID)

    @Column(name = "generation", nullable = false)
    public int generation = 1;

    @Column(name = "aggregate_id", nullable = false)
    public String aggregateId;

    @Column(name = "aggregate_type", nullable = false, length = 128)
    public String aggregateType;

    @Column(name = "bucket_id", nullable = false)
    public int bucketId;

    @Column(name = "destination_topic", nullable = false)
    public String destinationTopic;

    @Lob
    @Column(name = "payload", nullable = false)
    public String payload;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt = Instant.now();

    @Column(name = "retry_count", nullable = false)
    public int retryCount = 0;

    @Column(name = "next_retry_at", nullable = false)
    public Instant nextRetryAt = Instant.now();

    @Column(name = "last_error", length = 1024)
    public String lastError;
}
```

### 4.2. Entity: `OutboxLock` (`outbox_lock`)
Maintains dynamic virtual bucket leases for distributed pod workers.

```java
@Entity
@Table(name = "outbox_lock")
public class OutboxLock {

    @Id
    @Column(name = "bucket_id")
    public Integer bucketId;

    @Column(name = "locked_by", nullable = false)
    public String lockedBy;

    @Column(name = "expires_at", nullable = false)
    public Instant expiresAt;

    public OutboxLock() {}

    public OutboxLock(Integer bucketId, String lockedBy, Instant expiresAt) {
        this.bucketId = bucketId;
        this.lockedBy = lockedBy;
        this.expiresAt = expiresAt;
    }
}
```

#### Pre-Seeding Invariant:
Because standard JPQL does not support database-agnostic upserts (`ON CONFLICT DO UPDATE`), all $M$ virtual bucket rows (from `bucket_id = 0` to `M-1`, default $M = 1024$) **must be pre-seeded** in the database during schema creation or migration (ADR-002, ADR-003). Lock acquisition is then executed as a clean JPQL atomic Compare-And-Swap (CAS) update:

```sql
UPDATE OutboxLock l 
SET l.lockedBy = :podId, l.expiresAt = :newExpiry 
WHERE l.bucketId = :bucketId 
  AND (l.expiresAt < :now OR l.lockedBy = :podId)
```

### 4.3. Entity: `OutboxDeadLetter` (`outbox_dead_letter`)
Stores permanently failed events that exceed maximum retry thresholds.

```java
@Entity
@Table(name = "outbox_dead_letter")
public class OutboxDeadLetter {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    public UUID id; // Retains identical UUIDv7 from OutboxEvent

    @Column(name = "generation", nullable = false)
    public int generation;

    @Column(name = "aggregate_id", nullable = false)
    public String aggregateId;

    @Column(name = "aggregate_type", nullable = false, length = 128)
    public String aggregateType;

    @Column(name = "bucket_id", nullable = false)
    public int bucketId;

    @Column(name = "destination_topic", nullable = false)
    public String destinationTopic;

    @Lob
    @Column(name = "payload", nullable = false)
    public String payload;

    @Column(name = "failed_at", nullable = false)
    public Instant failedAt = Instant.now();

    @Column(name = "retry_count", nullable = false)
    public int retryCount;

    @Column(name = "exception_message", length = 1024)
    public String exceptionMessage;

    @Lob
    @Column(name = "stack_trace")
    public String stackTrace;

    @Column(name = "status", nullable = false, length = 32)
    public String status = "DEAD"; // DEAD, REPLAYED, SKIPPED
}
```

---

## 5. Virtual Bucketing & Online Generation Gating

To allow horizontal scalability across multiple Kubernetes pods while preserving strict chronological order for any domain aggregate, the outbox table is segmented into **Virtual Buckets** combined with **Online Generation/Epoch Gating**.

### 5.1. Deterministic Assignment
When the host application enqueues an event, the outbox interceptor assigns a `bucket_id` and `generation`:

$$bucket\_id = \left| \text{hash}(aggregate\_id) \right| \pmod M$$
$$generation = \text{configuredActiveGeneration}$$

Where:
- $M$ is the configurable virtual shard count (`quarkus.outbox.virtual-shards`, default: $1024$).
- `generation` is the configured topology epoch (`quarkus.outbox.generation`, default: $1$).
- `hash` uses a uniform distribution algorithm (e.g., standard Java `hashCode()` or Murmur3).
- If `aggregate_id` is null, a uniform pseudo-random bucket is assigned.

### 5.2. Online Generation Gating & Zero-Downtime Resharding
To avoid the "Hash Reshuffling Catastrophe" when scaling virtual bucket counts or Kafka partitions:
- The database table maintains composite index: `(generation ASC, bucket_id ASC, next_retry_at ASC, id ASC)`.
- When cluster administrators modify virtual bucket counts (e.g. from 1024 to 2048), they increment `quarkus.outbox.generation=2` in application configuration.
- Domain services immediately insert subsequent events stamped with `generation = 2` and bucketed across 2048 shards. Domain transactions never pause or wait.
- Workers evaluate the **Poller Prioritization Barrier**:
  ```sql
  SELECT COALESCE(MIN(e.generation), :activeGeneration) FROM OutboxEvent e
  ```
- While $\min(generation) < \text{activeGeneration}$, workers exclusively drain older generation records. Newer generation records accumulate safely in the database.
- Once the older generation drains to 0 rows, $\min(generation)$ evaluates to the active generation, instantly unblocking dispatching of the new topology without out-of-order anomalies.

### 5.3. 1:1 Lock Table Pre-Seeding
- The `outbox_lock` table is pre-seeded with $M$ rows ($bucket\_id = 0 \dots M-1$) covering the active generation's shard count.
- Each virtual bucket has its own independent lock row, eliminating hash collision bottlenecks between unrelated buckets.
- Application pod instances compete dynamically for active bucket locks using the greedy bin-packing engine.

---

## 6. Workload-Aware Fairness & Polling Engine

Instead of static bucket assignments, the polling engine applies **Lag-Weighted Multi-Fit Bin Packing**.

### 6.1. Backlog Estimation (O(1) Boundary Scanning)
To avoid database CPU degradation caused by `COUNT(*)` queries, backlog depth per bucket is estimated via index boundary gaps on `idx_outbox_gen_bucket_fifo`:

```sql
SELECT 
    bucket_id, 
    (MAX(id) - MIN(id) + 1) AS estimated_lag
FROM outbox_event
WHERE generation = :activeGeneration AND next_retry_at <= :now
GROUP BY bucket_id
ORDER BY estimated_lag DESC
```

### 6.2. Greedy Multi-Fit Bin Packing Algorithm
1. Each pod replica is initialized with a processing capacity budget:
   $$C_{\text{pod}} = \text{quarkus.outbox.pod.capacity} \quad (\text{default: } 500 \text{ messages})$$
2. At every polling interval, the pod queries the top active buckets ranked by `estimated_lag`.
3. The pod iterates through available candidate buckets and executes non-blocking `tryLock(bucketId, leaseTime)` calls.
4. For each acquired lock:
   $$\text{currentLoad} \leftarrow \text{currentLoad} + \max(1, \text{estimated\_lag})$$
5. When $\text{currentLoad} \ge C_{\text{pod}}$, the pod stops acquiring locks for the current cycle.
6. **Adaptive Shedding Rule:** If during execution a single bucket's actual batch size exceeds $C_{\text{pod}}$, the pod immediately releases its locks on all other buckets to let peer pods service them.

---

## 7. Kafka Dispatch Pipeline & CloudEvents

### 7.1. CloudEvents Binary Mode Specification
The extension dispatches events using the CloudEvents v1.0 Kafka Binary Content Mode to ensure downstream consumers can inspect routing and idempotency headers without deserializing the message body.

| CloudEvents Header | Value Source | Purpose |
| :--- | :--- | :--- |
| `ce_specversion` | `"1.0"` | CloudEvents specification standard |
| `ce_id` | `event.id.toString()` | Persistent UUIDv7 message ID for downstream deduplication |
| `ce_type` | `event.aggregateType` | Fully qualified domain event type |
| `ce_source` | `${quarkus.outbox.cloudevents.source}` | Emitting service identifier |
| `ce_time` | `ISO_INSTANT.format(event.createdAt)` | Event creation timestamp |
| `content-type` | `"application/json"` | Payload format descriptor |
| `x-outbox-bucket` | `String.valueOf(event.bucketId)` | Virtual bucket tracing header |
| `x-outbox-generation`| `String.valueOf(event.generation)` | Diagnostic topology epoch marker |

### 7.2. Zero-Allocation Dispatch Pipeline Standards
To adhere to the allocation budget ($\le 512\text{ bytes}$ per record, QG-5) and eliminate GC pressure:
1. **StatelessSession Record Projections:** Database batches must be fetched directly into immutable Java 21 `OutboxBatchRecord` instances:
   ```java
   List<OutboxBatchRecord> batch = session.createQuery(
       "SELECT new io.github.thoweber.quarkus.outbox.kafka.runtime.worker.OutboxBatchRecord(" +
       "e.id, e.generation, e.aggregateId, e.aggregateType, e.bucketId, e.destinationTopic, e.payload, e.createdAt) " +
       "FROM OutboxEvent e WHERE e.generation = :gen AND e.bucketId = :bucketId AND e.nextRetryAt <= :now " +
       "ORDER BY e.id ASC", OutboxBatchRecord.class)
       .setParameter("gen", activeGen)
       .setParameter("bucketId", bucketId)
       .setParameter("now", Instant.now())
       .setMaxResults(batchSize)
       .getResultList();
   ```
2. **Static Pre-Allocated Header Constants:** Header key byte arrays (`"ce_id"`, `"ce_type"`, etc.) must be defined as `static final byte[]` constants to eliminate duplicate UTF-8 conversions on every record.
3. **Zero-Copy Payload Forwarding:** Payload text/bytes must be passed directly to the Kafka `ProducerRecord` without allocating intermediate wrapper objects or performing unnecessary defensive copies.

### 7.3. Kafka Producer Configuration Invariants
To preserve per-partition ordering across Kafka brokers:
```properties
# Essential producer properties configured by the extension
kafka.enable.idempotence=true
kafka.acks=all
kafka.max.in.flight.requests.per.connection=5
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

### 7.4. Dedicated Bulkhead Lock DataSource Configuration
To prevent primary database traffic from causing lock lease expirations, the runtime establishes a dedicated Agroal connection pool for heartbeats and leases (ADR-005):

```properties
# Dedicated Lock DataSource (Heartbeats & CAS Leases Only)
quarkus.datasource."outbox-lock".db-kind=postgresql
quarkus.datasource."outbox-lock".jdbc.max-size=10
quarkus.datasource."outbox-lock".jdbc.min-size=2
quarkus.datasource."outbox-lock".jdbc.acquisition-timeout=3s
```

---

## 8. Failure Handling, Dead-Lettering & Aggregate Quarantining

### 8.1. Retry Backoff
When message dispatch fails (e.g., Kafka timeout, authorization error):
1. The transaction aborts.
2. The event's `retry_count` is incremented.
3. Next attempt is scheduled using exponential backoff:
   $$\text{delaySeconds} = \text{initialBackoff} \times (\text{multiplier}^{(\text{retryCount} - 1)})$$
4. The current bucket processing terminates immediately to preserve order.

### 8.2. Aggregate Quarantining Protocol
When an event exceeds `max_retries`:
1. The failed event is moved to `outbox_dead_letter` and deleted from `outbox_event`.
2. **Quarantine Invariant:** If subsequent events exist with the same `aggregate_id`, they must not be processed ahead of the failed event.
3. The engine automatically freezes pending events for the affected aggregate:
   ```sql
   UPDATE outbox_event 
   SET next_retry_at = '9999-12-31 23:59:59Z' 
   WHERE aggregate_id = :failedAggregateId
   ```
4. The remaining events in the bucket continue processing unimpeded.

### 8.3. Operator Admin REST API (`/q/outbox-kafka/admin`)
The extension provides secured administrative endpoints under the non-application path `/q/outbox-kafka/admin` for operational support:

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/q/outbox-kafka/admin/quarantined` | Lists all quarantined aggregate IDs and failure diagnostics. |
| `GET` | `/q/outbox-kafka/admin/quarantined/{id}/events` | Returns the dead-lettered event and the pending queue behind it. |
| `POST` | `/q/outbox-kafka/admin/quarantined/{id}/replay` | Resets `next_retry_at = now()` to retry processing after a system fix. |
| `POST` | `/q/outbox-kafka/admin/quarantined/{id}/skip` | Marks dead event as `SKIPPED` in `outbox_dead_letter` and unfreezes pending events. |
| `DELETE` | `/q/outbox-kafka/admin/quarantined/{id}` | Purges dead-letter and pending events for the specified aggregate. |
| `POST` | `/q/outbox-kafka/admin/drain` | Triggers graceful on-demand drainage of all pending bucket events. |

---

## 9. Configuration Property Reference

Mapped at runtime via `@ConfigMapping(prefix = "quarkus.outbox")`:

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | `true` | Enables/disables the outbox polling scheduler. |
| `generation` | `int` | `1` | Active topology generation epoch for online zero-downtime resharding. |
| `virtual-shards` | `int` | `1024` | Total virtual shards configured in DB ($M = 1024$). |
| `poll-interval` | `Duration` | `1s` | Interval between bucket poll executions. |
| `batch-size` | `int` | `50` | Maximum records retrieved in a single short-read transaction. |
| `pod.capacity` | `int` | `500` | Target maximum backlog processing budget per pod per cycle. |
| `lock.lease-duration`| `Duration` | `10s` | Time-to-live for a bucket lock lease. |
| `lock.strategy` | `String` | `table` | Lock implementation strategy: `table` or `postgres-advisory`. |
| `lock.datasource` | `String` | `outbox-lock` | Named Agroal DataSource bulkheading lock CAS updates and heartbeats. |
| `admin.path` | `String` | `outbox-kafka/admin` | Non-application subpath for admin REST API (resolves to `/q/outbox-kafka/admin`). |
| `dlq.max-retries` | `int` | `3` | Retry attempts before moving to Dead-Letter Sink. |
| `dlq.backoff.initial`| `Duration` | `2s` | Initial exponential backoff delay. |
| `dlq.backoff.multiplier`| `double`| `2.0` | Exponential backoff multiplier factor. |
| `cloudevents.source` | `String` | `/services/quarkus-outbox-kafka` | URI source attribute populated in CloudEvents headers. |

---

## 10. Module Layout & Build-Time Mechanics

```
quarkus-outbox-kafka/
├── pom.xml                               # Aggregator parent (io.github.thoweber:quarkus-outbox-kafka-parent)
├── deployment/                           # Build-time processor module
│   ├── pom.xml
│   └── src/main/java/io/github/thoweber/quarkus/outbox/kafka/deployment/
│       └── QuarkusOutboxKafkaProcessor.java  # Build steps for bytecode & bean generation
├── runtime/                              # Runtime execution module
│   ├── pom.xml
│   └── src/main/java/io/github/thoweber/quarkus/outbox/kafka/runtime/
│       ├── model/                        # JPA Entities: OutboxEvent, OutboxLock, OutboxDeadLetter
│       ├── poller/                       # WorkloadAwarePoller, BinPackingEngine
│       ├── worker/                       # StatelessBucketWorker, ShortTxManager
│       ├── dispatcher/                   # KafkaBatchDispatcher, CloudEventsMessageFactory
│       ├── spi/                          # OutboxLockProvider SPI, JpaTableLockProvider, LockHeartbeatSupervisor
│       └── admin/                        # OutboxAdminResource, DeadLetterHandler
└── integration-tests/                    # Dev Services end-to-end integration tests
    ├── pom.xml
    ├── TEST_GUIDE.md                     # Operational test guide & failure mode assertions
    └── src/test/java/io/github/thoweber/quarkus/outbox/kafka/it/
        ├── PhantomProducerIT.java        # GC pause & TOCTOU split-brain verification
        ├── ShortTransactionSandwichIT.java # Connection pool bulkhead verification
        ├── AggregateQuarantineIT.java     # Poison pill & trailing aggregate freeze
        └── OutboxConcurrencyIT.java      # Multi-pod contention & strict FIFO verification
```

### 10.1. Build Steps (`QuarkusOutboxKafkaProcessor.java`)
The extension registers entities and services at build time to support GraalVM native compilation:
1. **`AdditionalJpaModelBuildItem`:** Registers `OutboxEvent`, `OutboxLock`, and `OutboxDeadLetter` with Hibernate ORM so the consumer application does not need to configure scanning or a `persistence.xml`.
2. **`AdditionalBeanBuildItem`:** Registers `WorkloadAwarePoller`, `StatelessBucketWorker`, `JpaTableLockProvider`, and `OutboxAdminResource` as unremovable CDI beans.
3. **`FeatureBuildItem`:** Outputs `feature("quarkus-outbox-kafka")` into the startup banner.

---

## 11. Automated Verification & Test Harness Directives

The `integration-tests/` module implements comprehensive integration tests verifying core architectural invariants using Quarkus Dev Services (PostgreSQL + Kafka):

### 11.1. Core Test Suites

1. **`PhantomProducerIT` (GC Pause & Split-Brain Mitigation):**
   * Simulates a stalled pod by suspending a bucket worker thread during dispatch.
   * Verifies that peer pods steal the expired lease and dispatch pending records with persistent UUIDv7 `ce_id` headers.
   * Asserts that when the original pod resumes, its worker halts safely due to the $T_{\text{remaining}} < 2000\text{ms}$ buffer check.
   * Asserts that any inflight duplicate transmissions carry identical persistent `ce_id`s, allowing consumer-side idempotency filters to drop them without partition log sequence corruption.

2. **`ShortTransactionSandwichIT` (Database Connection Isolation):**
   * Injects network latency into the Kafka producer dispatch phase.
   * Inspects Agroal primary pool metrics (`quarkus.datasource.active-count`) during Kafka transit.
   * Asserts active database connection count drops to zero during the broker transit phase.

3. **`AggregateQuarantineIT` (Poison Pill & Trailing Event Isolation):**
   * Injects a serialization or broker failure into event 2 of 5 for `Aggregate-A`.
   * Asserts event 2 transitions to `outbox_dead_letter` with status `DEAD`.
   * Asserts events 3, 4, 5 for `Aggregate-A` have `next_retry_at` frozen at century-end (`9999-12-31`).
   * Asserts independent events for `Aggregate-B` in the same bucket drain completely.
   * Invokes `POST /q/outbox-kafka/admin/quarantined/Aggregate-A/replay` and asserts queue resumption.
   * Validates `POST /skip` and `DELETE /purge` endpoints.

4. **`OutboxConcurrencyIT` (Multi-Pod Active-Active Load & FIFO Ordering):**
   * Spawns multiple concurrent simulated worker pods against 1024 pre-seeded buckets.
   * Bulk produces 1,000 events across 50 distinct aggregates.
   * Validates 100% monotonic FIFO arrival per aggregate in Kafka topic consumers.

### 11.2. Verification Commands

Always run these commands before submitting changes:

```bash
# 1. Format code according to Quarkiverse conventions
./mvnw process-sources
git diff --exit-code

# 2. Fast compile and unit verification
./mvnw clean test-compile

# 3. Full integration verification with Dev Services
./mvnw clean verify

# 4. GraalVM native binary verification
./mvnw verify -pl integration-tests -Dnative
```
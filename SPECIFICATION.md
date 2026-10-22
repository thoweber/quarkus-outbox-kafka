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
| **Target Coordinates** | Group: `io.quarkiverse.outbox` (or `io.github.<user>`), Artifact: `quarkus-outbox-parent` |
| **Persistence Engine** | Hibernate ORM 6.x (`quarkus-hibernate-orm`), `StatelessSession` |
| **Messaging Client** | Quarkus Kafka Client (`quarkus-kafka-client` or SmallRye Reactive Messaging) |

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
- Distributed lock state checks and background heartbeats must execute on an isolated thread pool/connection channel to prevent lock lease expiration caused by worker I/O stalls or database connection pool exhaustion.
- If a heartbeat lease renewal fails, the worker must abort its current batch immediately before releasing the lease.

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
        @Index(name = "idx_outbox_bucket_fifo", columnList = "bucket_id, next_retry_at, id ASC"),
        @Index(name = "idx_outbox_aggregate", columnList = "aggregate_id")
    }
)
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

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
Because standard JPQL does not support database-agnostic upserts (`ON CONFLICT DO UPDATE`), all $N$ bucket rows (from `bucket_id = 0` to `N - 1`) **must be pre-seeded** in the database during schema creation or migration. Lock acquisition is then executed as a clean JPQL atomic Compare-And-Swap (CAS) update:

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
    public Long id;

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

## 5. Virtual Bucketing & Partition Allocation

To allow horizontal scalability across multiple Kubernetes pods while preserving strict chronological order for any domain aggregate, the outbox table is segmented into **Virtual Buckets**.

### 5.1. Deterministic Assignment
When the host application enqueues an event, the outbox interceptor assigns a `bucket_id`:

$$bucket\_id = \left| \text{hash}(aggregate\_id) \right| \pmod M$$

Where:
- $M$ is the fixed virtual shard count (default: $1024$ or $256$).
- `hash` uses a uniform distribution algorithm (e.g., standard Java `hashCode()` or Murmur3).
- If `aggregate_id` is null, a uniform pseudo-random bucket is assigned.

### 5.2. Pre-Sharding Decoupling
To avoid schema migrations when Kafka partition counts increase:
- The database schema sets virtual buckets to a fixed high number $M = 1024$.
- The number of concurrent worker locks $L$ is configurable independently ($L \le M$).
- Lock $i$ protects virtual buckets $B$ where $B \pmod L = i$.

---

## 6. Workload-Aware Fairness & Polling Engine

Instead of static bucket assignments, the polling engine applies **Lag-Weighted Multi-Fit Bin Packing**.

### 6.1. Backlog Estimation (O(1) Boundary Scanning)
To avoid database CPU degradation caused by `COUNT(*)` queries, backlog depth per bucket is estimated via index boundary gaps on `idx_outbox_bucket_fifo`:

```sql
SELECT 
    bucket_id, 
    (MAX(id) - MIN(id) + 1) AS estimated_lag
FROM outbox_event
WHERE next_retry_at <= :now
GROUP BY bucket_id
ORDER BY estimated_lag DESC
```

### 6.2. Greedy Multi-Fit Bin Packing Algorithm
1. Each pod replica is initialized with a processing capacity budget:
   $$C_{\text{pod}} = \text{quarkus.outboxkafka.pod.capacity} \quad (\text{default: } 500 \text{ messages})$$
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
| `ce_id` | `String.valueOf(event.id)` | Immutable message ID for downstream deduplication |
| `ce_type` | `event.aggregateType` | Fully qualified domain event type |
| `ce_source` | `${quarkus.outboxkafka.cloudevents.source}` | Emitting service identifier |
| `ce_time` | `ISO_INSTANT.format(event.createdAt)` | Event creation timestamp |
| `content-type` | `"application/json"` | Payload format descriptor |
| `x-outbox-bucket` | `String.valueOf(event.bucketId)` | Virtual bucket tracing header |

### 7.2. Kafka Producer Configuration Invariants
To preserve per-partition ordering across Kafka brokers:
```properties
# Essential producer properties configured by the extension
kafka.enable.idempotence=true
kafka.acks=all
kafka.max.in.flight.requests.per.connection=5
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer
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

### 8.3. Operator Admin REST API (`/q/outbox-admin`)
The extension provides secured administrative endpoints for operational support:

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/q/outbox-admin/quarantined` | Lists all quarantined aggregate IDs and failure diagnostics. |
| `GET` | `/q/outbox-admin/quarantined/{id}/events` | Returns the dead-lettered event and the pending queue behind it. |
| `POST` | `/q/outbox-admin/quarantined/{id}/replay` | Resets `next_retry_at = now()` to retry processing after a system fix. |
| `POST` | `/q/outbox-admin/quarantined/{id}/skip` | Marks dead event as `SKIPPED` and unfreezes pending events. |
| `POST` | `/q/outbox-admin/quarantined/{id}/purge` | Purges all pending events for the specified aggregate. |

---

## 9. Configuration Property Reference

Mapped at runtime via `@ConfigMapping(prefix = "quarkus.outboxkafka")`:

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | `true` | Enables/disables the outbox polling scheduler. |
| `generation` | `int` | `1` | Configuration epoch for dynamic bucket re-sharding. |
| `buckets.count` | `int` | `16` | Number of active lock buckets (must divide or map to virtual shards). |
| `virtual-shards` | `int` | `1024` | Total virtual shards configured in DB. |
| `poll-interval` | `Duration` | `1s` | Interval between bucket poll executions. |
| `batch-size` | `int` | `50` | Maximum records retrieved in a single short-read transaction. |
| `pod.capacity` | `int` | `500` | Target maximum backlog processing budget per pod per cycle. |
| `lock.lease-duration`| `Duration` | `10s` | Time-to-live for a bucket lock lease. |
| `lock.strategy` | `String` | `table` | Lock implementation strategy: `table` or `postgres-advisory`. |
| `dlq.max-retries` | `int` | `3` | Retry attempts before moving to Dead-Letter Sink. |
| `dlq.backoff.initial`| `Duration` | `2s` | Initial exponential backoff delay. |
| `dlq.backoff.multiplier`| `double`| `2.0` | Exponential backoff multiplier factor. |
| `cloudevents.source` | `String` | `/services/quarkus-outbox` | URI source attribute populated in CloudEvents headers. |

---

## 10. Module Layout & Build-Time Mechanics

```
quarkus-outbox/
├── pom.xml                               # Aggregator parent (inherits io.quarkiverse:quarkiverse-parent)
├── deployment/                           # Build-time processor module
│   ├── pom.xml
│   └── src/main/java/.../deployment/
│       └── OutboxProcessor.java          # Build steps for bytecode & bean generation
├── runtime/                              # Runtime execution module
│   ├── pom.xml
│   └── src/main/java/.../runtime/
│       ├── model/                        # JPA Entities: OutboxEvent, OutboxLock, OutboxDeadLetter
│       ├── poller/                       # WorkloadAwarePoller, BinPackingEngine
│       ├── worker/                       # StatelessBucketWorker, ShortTxManager
│       ├── dispatcher/                   # KafkaDispatcher, CloudEventsMessageFactory
│       ├── spi/                          # OutboxLockProvider SPI, TableBasedLockProvider
│       └── admin/                        # OutboxAdminResource, QuarantineManager
└── integration-tests/                    # Dev Services end-to-end integration tests
    ├── pom.xml
    └── src/test/java/.../
        └── OutboxConcurrencyIT.java      # PostgreSQL + Kafka testcontainer validation
```

### 10.1. Build Steps (`OutboxProcessor.java`)
The extension registers entities and services at build time to support GraalVM native compilation:
1. **`AdditionalJpaModelBuildItem`:** Registers `OutboxEvent`, `OutboxLock`, and `OutboxDeadLetter` with Hibernate ORM so the consumer application does not need to configure scanning or a `persistence.xml`.
2. **`AdditionalBeanBuildItem`:** Registers `WorkloadAwarePoller`, `StatelessBucketWorker`, `TableBasedLockProvider`, and `OutboxAdminResource` as unremovable CDI beans.
3. **`FeatureBuildItem`:** Outputs `feature("outbox-kafka")` into the startup banner.

---

## 11. Verification & Testing Checklist for Agents

Before completing any code modifications:

1. **Source Formatting:**
   ```bash
   ./mvnw process-sources
   git diff --exit-code
   ```
   *Note: Must strictly comply with Quarkus formatter conventions.*

2. **Compile & Unit Verification:**
   ```bash
   ./mvnw clean test-compile
   ```

3. **Dev Services Concurrency Test:**
   ```bash
   ./mvnw verify -pl integration-tests
   ```
   *Validates concurrent multi-pod contention, lease expiry, and Kafka publishing using real Testcontainers.*

4. **GraalVM Native Build Validation:**
   ```bash
   ./mvnw verify -pl integration-tests -Dnative
   ```
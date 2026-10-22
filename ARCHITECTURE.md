# Architecture Documentation: Quarkus Transactional Outbox (`quarkus-outbox-kafka`)

This document adheres to the [arc42](https://arc42.org/) architectural documentation template. It provides a comprehensive specification of the system's structural boundaries, runtime lifecycles, concurrency protocols, and operational guardrails.

---

## 1. Introduction and Goals

The `quarkus-outbox-kafka` extension provides a production-grade, database-agnostic implementation of the **Transactional Outbox Pattern** tailored for Quarkus microservices running on Java 21 LTS. It resolves the classic dual-write distributed systems problem when updating relational databases and publishing events to Apache Kafka.

Unlike heavy Change Data Capture (CDC) pipelines that require Debezium connectors and database Write-Ahead Log (WAL) replication privileges, or naive single-worker polling solutions that cause horizontal scaling bottlenecks, `quarkus-outbox-kafka` provides active-active horizontal scaling across Kubernetes pods using **Virtual Bucketing** with workload-aware greedy bin-packing, while guaranteeing strict FIFO event ordering per aggregate identifier.

```
                        THE OUTBOX SPECTRUM
   Heavy Infrastructure                         Naive Polling
   ◄──────────────────────────────────────────────────────────►
   Debezium / CDC                               Single-Lock Polling
   - Demands Kafka Connect                      - 1 Pod works, N-1 idle
   - DBA REPLICATION grants                     - Destroys batching
   - High operational cost                      - Breaks FIFO ordering
                                  ▲
                                  │
                   [quarkus-outbox-kafka Extension]
                   - Zero external infrastructure
                   - Active-active multi-pod scale
                   - 100% database-agnostic (ANSI JPA)
                   - Native Kafka batching & CloudEvents
```

### 1.1 Requirements Overview

* **Atomic Dual-Write Guarantee:** Application state updates and outbox event persistence occur within the same local ACID database transaction.
* **Per-Aggregate FIFO Ordering:** Outbox events sharing an identical `aggregateId` are dispatched to Kafka strictly in creation sequence, even across pod rebalances and rolling deployments.
* **Active-Active Scalability:** Multiple microservice pods concurrently drain the same outbox table without lock thrashing or double dispatch.
* **Database Portability:** Complete portability across PostgreSQL, MySQL, MariaDB, Oracle Database, Microsoft SQL Server, and H2 without proprietary database extensions, native upserts, or log-tailing triggers.
* **Network & Connection Isolation ("Short-Transaction Sandwich"):** Zero database connections or locks are held open during network socket interactions with Kafka brokers.
* **Idempotency & Tracing:** Outbox records carry standard CloudEvents v1.0 binary metadata to support end-to-end downstream consumer deduplication.
* **Fine-Grained Quarantining & Dead-Letter Isolation:** Unprocessable events (poison pills) automatically transition to a persistent dead-letter queue, freezing only the affected aggregate while peer traffic continues unimpeded.

### 1.2 Quality Goals

The architecture establishes concrete, measurable performance, resource efficiency, and correctness SLAs adhering to ISO/IEC 25010 standards:

#### 1.2.1 Throughput & Latency (I/O Efficiency)
| Goal ID | Quality Attribute | Metric | Target SLA | Measurement Method |
| :---: | :--- | :--- | :--- | :--- |
| **QG-1** | **Drain Throughput (Single Pod)** | Events dispatched / second | $\ge \mathbf{10{,}000}\text{ msgs/sec}$ (batch size 100, 1KB payload, PostgreSQL 16 + Kafka 3.7) | JMH benchmark / Micrometer `outbox.dispatch.throughput` |
| **QG-2** | **End-to-End Transit Latency** | Time from DB commit to Kafka broker ACK | **p50:** $\le \mathbf{15\text{ms}}$<br>**p95:** $\le \mathbf{50\text{ms}}$<br>**p99:** $\le \mathbf{100\text{ms}}$ | Micrometer timer `outbox.event.transit.duration` |
| **QG-3** | **Short-Transaction Hold Time** | Active database connection borrow duration | **Phase 1 (Select):** $\le \mathbf{3\text{ms}}$ (p99)<br>**Phase 2 (Kafka):** $\mathbf{0\text{ms}}$ (hard zero)<br>**Phase 3 (Delete):** $\le \mathbf{5\text{ms}}$ (p99) | Agroal connection acquisition & hold metrics |
| **QG-4** | **Poller Boundary Scan Overhead** | Time to estimate backlog depth across all active buckets | **p99:** $\le \mathbf{2\text{ms}}$ (via composite index `(generation, bucket_id, next_retry_at, id)`) | Timer on `WorkloadAwarePoller.estimateBacklog()` |

#### 1.2.2 GC Pressure & Allocation Efficiency (Low Overhead)
| Goal ID | Quality Attribute | Metric | Target SLA | Measurement Method |
| :---: | :--- | :--- | :--- | :--- |
| **QG-5** | **Heap Allocation Rate** | Bytes allocated per dispatched outbox record | $\le \mathbf{512\text{ bytes}}$ per record (excluding Kafka producer internal socket buffer) | Async-profiler / JFR (`jdk.ObjectAllocationInNewTLAB`) |
| **QG-6** | **Zero Persistence Hydration Churn** | Entity hydration allocations in dispatch loop | **0 managed entity objects** (exclusively use `StatelessSession` direct constructor / Record projections) | Heap allocation profiling / JFR object count |
| **QG-7** | **GC Pause Budget** | Maximum Stop-The-World (STW) pause duration during continuous peak load | **p99.9:** $\le \mathbf{10\text{ms}}$ (ZGC / Shenandoah)<br>**Max STW:** $< \mathbf{50\text{ms}}$ (G1 GC) | GC logs / Micrometer `jvm.gc.pause` |

#### 1.2.3 Memory Footprint & Resource Utilization (Compact Resource Envelope)
| Goal ID | Quality Attribute | Metric | Target SLA | Measurement Method |
| :---: | :--- | :--- | :--- | :--- |
| **QG-8** | **JVM Heap Overhead** | Dedicated extension heap consumption at steady state | $\le \mathbf{32\text{ MB}}$ (excluding user payload Kafka socket accumulator) | Java Flight Recorder / JConsole live heap retained size |
| **QG-9** | **GraalVM Native Image RSS** | Total container Resident Set Size under steady peak load | $\le \mathbf{64\text{ MB}}$ baseline; $\le \mathbf{96\text{ MB}}$ under peak throughput ($10\text{k}$ msgs/sec) | Linux `/proc/[pid]/status` `VmRSS` |
| **QG-10**| **Concurrency Stack Overhead** | Memory consumed by supervisors and heartbeat tasks | $\le \mathbf{1\text{ KB}}$ per virtual thread stack (using Java 21 `Thread.ofVirtual()`) | JVM Virtual Thread thread dump inspection |
| **QG-11**| **In-Flight Memory Bound** | Maximum bounded memory for uncommitted batches | $\le \mathbf{5\text{ MB}}$ per pod (enforced via $C_{\text{pod}} = 500$ messages $\times$ max batch size) | Bounded queue multi-fit assertion |

#### 1.2.4 Reliability & Correctness SLAs
| Goal ID | Quality Attribute | Metric | Target SLA | Measurement Method |
| :---: | :--- | :--- | :--- | :--- |
| **QG-12**| **Aggregate FIFO Sequencing** | Out-of-sequence deliveries per aggregate ID | **Strictly 0 anomalies** ($100\%$ monotonic sequence delivery) | `OutboxConcurrencyIT` Kafka consumer sequence validation |
| **QG-13**| **Duplicate Delivery Deduplication** | Consumer deduplication rate via persistent UUIDv7 `ce_id` during pod failover | $\mathbf{100\%}$ deduplicable ($0$ untracked duplicates) | `PhantomProducerIT` consumer-side assertion |
| **QG-14**| **Heartbeat Isolation Under Starvation** | Bucket lock heartbeat success rate when primary connection pool is $100\%$ saturated | $\mathbf{100\%}$ heartbeat success within $\frac{1}{3} \times T_{\text{lease}}$ | `ShortTransactionSandwichIT` pool saturation stress test |

### 1.3 Stakeholders

| Role | Name | Expectations |
| :--- | :--- | :--- |
| **Microservice Developers** | Extension Consumers | Clean, idiomatic Quarkus integration; zero manual lock or reflection configuration; simple API for event persistence. |
| **Platform / DevOps Engineers** | Cluster Operators | No extra infrastructure requirements (no Kafka Connect clusters or Debezium operators); seamless Kubernetes multi-pod scaling; native health checks and metrics. |
| **Database Administrators (DBAs)** | Data Tier Custodians | No administrative privileges (`SUPERUSER`, `REPLICATION`), no persistent database connections held open during network calls, no vendor lock-in. |
| **Downstream Consumers** | Domain Service Teams | Reliable CloudEvents v1.0 binary mode envelopes with guaranteed monotonic per-aggregate sequence delivery. |

---

## 2. Architecture Constraints

### 2.1 Technical Constraints

* **Platform Baseline:** Target compilation against **Java 21 LTS** (`<maven.compiler.release>21</maven.compiler.release>`). Must utilize Java 21 features (Virtual Threads, Sequenced Collections, Record Patterns) while maintaining strict compatibility with Quarkus 3.15+ and Quarkus 4.
* **Quarkus Extension Paradigm:** Strict separation between build-time deployment logic (`deployment/`) and runtime logic (`runtime/`). Jandex indexing, bytecode augmentation, and synthetic bean registration must take place during deployment augmentations.
* **Standard Persistence:** All persistence operations must strictly use standard **Jakarta Persistence (JPA / JPQL)** constructs and Hibernate `StatelessSession`. Vendor-specific native SQL, stored procedures, or dialect-dependent types (`JSONB`, `BYTEA`) are forbidden in default components.
* **Kafka Client:** Direct dependency on `io.quarkus:quarkus-kafka-client` (`org.apache.kafka.clients.producer.KafkaProducer`). Avoid SmallRye Reactive Messaging channel abstractions to ensure dynamic topic routing, low-allocation record construction, and direct access to Kafka's batch accumulator and error callbacks.

### 2.2 Organizational Constraints

* **Ecosystem Governance:** Inherits from `io.quarkiverse:quarkiverse-parent` to ensure automated code formatting (`impsort-maven-plugin`, `formatter-maven-plugin`), Apache 2.0 license header enforcement, and streamlined staging to Maven Central.
* **Testing Rigor:** 100% of integration test suites must run against real database engines and real Kafka clusters via Quarkus Dev Services (Testcontainers), validating both standard JVM and GraalVM native binary execution.

---

## 3. Context and Scope

### 3.1 Business Context

The following diagram places the `quarkus-outbox-kafka` extension in the context of the business application, its storage layers, and surrounding infrastructure:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Quarkus Microservice Pod                        │
│                                                                        │
│   ┌─────────────────────┐                 ┌────────────────────────┐   │
│   │   Domain Business   │                 │   quarkus-outbox-kafka    │   │
│   │       Services      │──(Write Event)─►│    Runtime Engine      │   │
│   └──────────┬──────────┘                 └─────┬────────────┬─────┘   │
└──────────────┼──────────────────────────────────┼────────────┼─────────┘
               │ (Local ACID Transaction)         │            │
               ▼                                  ▼            │
    ┌──────────────────────┐             ┌─────────────┐       │ (Network Batch
    │ Primary Application  │             │ Dedicated   │       │  Produce)
    │  Database (Entities) │             │ Lock Table  │       │
    │  - outbox_event      │             │ outbox_lock │       ▼
    │  - outbox_dlq        │             └─────────────┘ ┌───────────────┐
    └──────────────────────┘                             │ Apache Kafka  │
                                                         │ Broker Tier   │
                                                         └───────┬───────┘
                                                                 │
                                                                 ▼
                                                         ┌───────────────┐
                                                         │  Downstream   │
                                                         │ Event Stream  │
                                                         │   Consumers   │
                                                         └───────────────┘
```

| Neighbor System / Actor | Channel / Protocol | Purpose / Interaction |
| :--- | :--- | :--- |
| **Domain Business Service** | Internal Java API / CDI | Writes business entities and appends `OutboxEvent` records within the local transaction. |
| **Primary Relational DB** | JDBC / ANSI JPQL | Stores business domain entities, pending `OutboxEvent` records, and quarantined dead-letter records. |
| **Lock Data Source** | JDBC / ANSI JPQL | Dedicated connection pool managing atomic bucket leases in `outbox_lock` via CAS queries. |
| **Apache Kafka Broker** | Kafka Wire Protocol (TCP) | Target messaging system receiving dispatched outbox records with CloudEvents headers. |
| **Operations Engineer / Admin** | HTTP / JSON (`/q/outbox-kafka/admin`) | Inspects quarantined aggregates, triggers event replay, skips poison pills, or purges queues. |

### 3.2 Technical Context

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                               Quarkus Host Pod                                  │
│                                                                                 │
│   [Agroal Primary Pool]      [Agroal Lock Pool]      [Native KafkaProducer]     │
│             │                        │                         │                │
└─────────────┼────────────────────────┼─────────────────────────┼────────────────┘
              │                        │                         │
      JDBC (ANSI JPQL)         JDBC (ANSI JPQL)            TCP / Kafka Protocol
              │                        │                         │
              ▼                        ▼                         ▼
   ┌──────────────────────┐ ┌──────────────────────┐  ┌───────────────────────┐
   │ Relational Database  │ │ Relational Database  │  │     Apache Kafka      │
   │   (Default Schema)   │ │   (Lock Bulkhead)    │  │       Cluster         │
   └──────────────────────┘ └──────────────────────┘  └───────────────────────┘
```

The system segregates network and database interactions into three isolated I/O pipelines:
1. **Application Persistence Pipeline:** Reads and deletes outbox events using Hibernate's high-speed `StatelessSession`.
2. **Bulkhead Coordination Pipeline:** Periodically renews leases using a dedicated Agroal connection pool (`outbox-lock`) to isolate heartbeats from application connection pool starvation.
3. **Kafka Transport Pipeline:** Transmits buffered message batches asynchronously via the native `KafkaProducer`, keeping zero database connections open.

---

## 4. Solution Strategy

1. **Virtual Bucketing with Online Generation/Epoch Gating ($M = 1024$ default):**
   * Decouples physical database sharding from both Kafka partition counts and worker pod counts.
   * Deterministically routes aggregate events using positive hashing:
     $$\text{bucketId} = |\text{hashCode}(\text{aggregateId})| \pmod M$$
   * Zero-downtime resharding is orchestrated via generation/epoch gating: new events write to `generation = G + 1` with new bucket counts while poller barrier `SELECT MIN(generation)` ensures older generation events drain to 0 before newer events dispatch.
   * Reduces the blast radius of a noisy neighbor to $\le 0.098\%$ of the key space.
2. **Persistent UUIDv7 & Consumer-Side Idempotency Contract:**
   * Primary key and CloudEvents `ce_id` are mapped strictly and immutably from persistent **UUIDv7** (RFC 9562).
   * Time-ordered 48-bit millisecond prefixes maintain B-tree database index performance and chronological sorting while providing globally unique identifiers that downstream consumers use for replay deduplication.
3. **The "Short-Transaction Sandwich":**
   * Eliminates the risk of long-lived database transactions holding locks across network boundaries.
   * Splits processing into three non-overlapping phases: **Short Read TX** $\to$ **Network Send (0 DB Connections)** $\to$ **Short Write TX**.
4. **Pre-Seeded Locks with JPQL Compare-And-Swap (CAS):**
   * Tables are pre-seeded with $M$ rows ($0 \dots M-1$) upon startup.
   * Lock acquisition and release use atomic, vendor-agnostic JPQL conditional updates (`UPDATE outbox_lock SET locked_by = :node WHERE expires_at < :now`), avoiding proprietary SQL upserts.
5. **Workload-Aware Greedy Bin-Packing:**
   * Pods query pending bucket backlogs using fast $O(1)$ composite index boundary scans `(MAX(id) - MIN(id) + 1)`, dynamically claiming buckets up to a configured processing budget ($C$).
6. **Fail-Fast Virtual Thread Heartbeat Supervision & Worker Deadline Buffer:**
   * A dedicated supervisor running on a Java 21 virtual thread renews the active bucket lease every $\frac{1}{3} \times T_{\text{lease}}$.
   * If a heartbeat fails or database connectivity stalls past $\frac{2}{3} \times T_{\text{lease}}$, a volatile `leaseLost` flag is raised and the worker thread is interrupted before the lease expires.
   * Workers also check a cooperative fail-fast deadline buffer ($T_{\text{remaining}} \ge 2000\text{ms}$) before initiating each message dispatch, preventing split-brain dual dispatch during Stop-The-World GC pauses.
7. **Aggregate Quarantining:**
   * Poison pills reaching `maxRetries` are moved to `outbox_dead_letter`.
   * Subsequent events for that specific `aggregateId` are frozen (`next_retry_at = '9999-12-31'`), protecting sequence consistency while letting the rest of the virtual bucket proceed.

---

## 5. Building Block View

### 5.1 Level 1: Multi-Module Architecture

The repository is structured into four distinct modules respecting Quarkus extension conventions:

```
quarkus-outbox-kafka/
├── deployment/           # Build-time augmentation, bytecode processors, Jandex indexers
├── runtime/              # CDI beans, JPA entities, lock providers, Kafka dispatchers
├── integration-tests/    # Native & JVM test suites running against Dev Services
└── docs/                 # Antora documentation source
```

```
┌────────────────────────────────────────────────────────┐
│               Deployment Module (Build Time)           │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │                 OutboxProcessor                  │  │
│  │ - Registers Feature: "outbox-virtual-bucketing"  │  │
│  │ - Produces AdditionalJpaModelBuildItem           │  │
│  │ - Produces AdditionalBeanBuildItem (Unremovable) │  │
│  │ - Generates Bytecode Facilitators                │  │
│  └──────────────────────────────────────────────────┘  │
└───────────────────────────┬────────────────────────────┘
                            │ Augments & Configures
                            ▼
┌────────────────────────────────────────────────────────┐
│                 Runtime Module (Execution)             │
│                                                        │
│  ┌──────────────────┐ ┌──────────────────┐ ┌─────────┐ │
│  │ Poller & Worker  │ │ Lock & Heartbeat │ │ Model   │ │
│  │ Orchestration    │ │ SPI Providers    │ │ Layer   │ │
│  └──────────────────┘ └──────────────────┘ └─────────┘ │
└────────────────────────────────────────────────────────┘
```

### 5.2 Level 2: Runtime Subsystems

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 Runtime Module Components                               │
│                                                                                         │
│  ┌─────────────────────────┐      ┌────────────────────────┐      ┌──────────────────┐  │
│  │   WorkloadAwarePoller   │─────►│    JpaBucketWorker     │─────►│  KafkaDispatcher │  │
│  │ - Greedy Bin-Packing    │      │ - StatelessSession     │      │ - CloudEvents    │  │
│  │ - Index Boundary Scans  │      │ - Sandwich Lifecycle   │      │ - Native Producer│  │
│  └───────────┬─────────────┘      └───────────┬────────────┘      └──────────────────┘  │
│              │                                │                                         │
│              ▼                                ▼                                         │
│  ┌─────────────────────────┐      ┌────────────────────────┐      ┌──────────────────┐  │
│  │   OutboxLockProvider    │      │   DeadLetterHandler    │      │ OutboxAdminAPI   │  │
│  │ - JpaTableLockProvider  │      │ - Aggregate Quarantine │      │ - /q/outbox-kafka│  │
│  │ - Virtual Thread Super. │      │ - Exponential Backoff  │      │   /admin         │  │
│  └─────────────────────────┘      └────────────────────────┘      └──────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 5.2.1 Component Specifications

| Component | Scope | Responsibility |
| :--- | :--- | :--- |
| **`WorkloadAwarePoller`** | `@ApplicationScoped` | Periodically samples candidate buckets using index boundary probes, greedily claims leases within capacity budget $C$, and delegates to workers. |
| **`JpaBucketWorker`** | `@ApplicationScoped` | Coordinates the three-phase "Short-Transaction Sandwich" for a single leased bucket using Hibernate `StatelessSession`. |
| **`JpaTableLockProvider`** | `@ApplicationScoped` | Default implementation of `OutboxLockProvider` executing atomic JPQL CAS updates against the pre-seeded `outbox_lock` table. |
| **`LockHeartbeatSupervisor`** | Per-Lease Execution | Lightweight virtual thread (`Thread.ofVirtual()`) that renews the bucket lease at fixed intervals ($\frac{1}{3} \times T_{\text{lease}}$) and sets `leaseLost = true` if renewal slips past $\frac{2}{3} \times T_{\text{lease}}$. |
| **`KafkaBatchDispatcher`** | `@ApplicationScoped` | Enriches event payloads with CloudEvents v1.0 binary mode headers and dispatches them via `org.apache.kafka.clients.producer.KafkaProducer`, collecting broker ACKs asynchronously in-memory. |
| **`DeadLetterHandler`** | `@ApplicationScoped` | Manages retry state updates, writes poisoned events to `outbox_dead_letter`, and quarantines affected aggregates via `Transactional.TxType.REQUIRES_NEW`. |
| **`OutboxAdminResource`** | `@Path("/q/outbox-kafka/admin")` | Secured REST management API enabling operators to query, unfreeze, replay, skip, or purge quarantined aggregates. |

---

## 6. Runtime View

### 6.1 Scenario 1: The "Short-Transaction Sandwich" Dispatch Lifecycle

This scenario details the primary execution path for dispatching a batch of outbox events without holding open database locks or connections during Kafka I/O.

```
Worker Pod            Primary DB (StatelessSession)     Kafka Broker
    │                             │                          │
    ├─► Phase 1: Short Read TX    │                          │
    │   BEGIN TX                  │                          │
    │   SELECT batch FROM outbox  │                          │
    │   COMMIT TX (Close Conn) ──►│                          │
    │                             │                          │
    ├─► Phase 2: Network Send (0 DB Connections)             │
    │   Iterate & format CloudEvents                         │
    │   producer.send(record) ──────────────────────────────►│
    │   Collect in-memory ACKs ◄─────────────────────────────┤ (Broker acks=all)
    │                                                        │
    ├─► Phase 3: Short Write TX   │                          │
    │   BEGIN TX                  │                          │
    │   DELETE FROM outbox        │                          │
    │     WHERE id IN (:ackIds)   │                          │
    │   COMMIT TX (Close Conn) ──►│                          │
    ▼                             ▼                          ▼
```

1. **Short Read Transaction:**
   * The worker opens a short transaction via `QuarkusTransaction.requiringNew()`.
   * Using a Hibernate `StatelessSession`, it fetches up to `batchSize` records ordered by `id ASC` where `bucket_id = :bId AND next_retry_at <= :now`.
   * The transaction commits immediately. The database connection is returned to the Agroal pool.
2. **Network Dispatch Phase (Zero Database Connections):**
   * The worker iterates through the in-memory batch.
   * For each event, it verifies that the `leaseLost` flag remains false.
   * Payloads are transformed into Kafka `ProducerRecord` instances decorated with CloudEvents headers.
   * Records are dispatched to Kafka via `KafkaProducer.send()`. Broker ACKs and exceptions are collected in-memory.
3. **Short Write Transaction:**
   * The worker opens a second isolated transaction.
   * A single bulk JPQL statement is executed:
     $$\text{DELETE FROM OutboxEvent e WHERE e.id IN (:successfulIds)}$$
   * Any failed records are routed to the `DeadLetterHandler`.
   * The transaction commits and releases the connection.

### 6.2 Scenario 2: Lock Acquisition, Virtual Thread Heartbeat, and Fail-Fast Safety

```
Poller Thread               Lock DB Pool            Virtual Thread (Heartbeat)        Worker Loop
    │                            │                              │                          │
    ├─► tryLock(bucketId)        │                              │                          │
    │   CAS UPDATE outbox_lock ─►│                              │                          │
    │   (Success: Rows = 1)      │                              │                          │
    │                            │                              │                          │
    ├─► Spawn Heartbeat Thread ──┼─────────────────────────────►│                          │
    │   (Virtual Thread)         │                              │                          │
    │                            │                              │  loop every 1/3 lease:   │
    ├─► Execute Worker ──────────┼──────────────────────────────┼─────────────────────────►│
    │                            │                              │  UPDATE lease expires_at │
    │                            │   [Database Slowdown / Stalls]                          │
    │                            │◄─────────────────────────────┤                          │
    │                            │   Connection Timeout / Drop  │                          │
    │                            │                              │                          │
    │                            │                              │  IF elapsed > 2/3 lease: │
    │                            │                              ├─► leaseLost = true       │
    │                            │                              ├─► workerThread.interrupt()
    │                            │                              │                          │
    │                            │                              │                          ├─► Checks leaseLost
    │                            │                              │                          │   ABORTS BATCH
    ▼                            ▼                              ▼                          ▼   (Prevents Split-Brain)
```

1. **CAS Lock Claim:** The poller issues a single atomic query against `outbox_lock`:
   ```sql
   UPDATE OutboxLock l 
   SET l.lockedBy = :nodeId, l.expiresAt = :now + :leaseDuration 
   WHERE l.bucketId = :bucketId 
     AND (l.lockedBy = :nodeId OR l.expiresAt < :now)
   ```
2. **Heartbeat Spawn:** Upon claiming the row, a Java 21 virtual thread starts a recurring heartbeat loop at interval $T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$.
3. **Fail-Fast Boundary:** If a database pause, GC freeze, or pool exhaustion prevents lease renewal past $\frac{2}{3} \times T_{\text{lease}}$:
   * The supervisor marks `volatile boolean leaseLost = true`.
   * The supervisor calls `workerThread.interrupt()`.
4. **Worker Defense:** Before sending each record, the worker checks `if (leaseLost) throw new LeaseLostException()`. Dispatch terminates instantly, leaving trailing messages in the database and preventing split-brain dual dispatch.

### 6.3 Scenario 3: Poison Pill & Aggregate Quarantining

```
Worker                   Kafka Broker               Primary DB (Requires New)
  │                           │                                │
  ├─► producer.send(Event-10) │                                │
  │   (Aggregate: "Order-99") │                                │
  │◄── Broker Error (NACK) ───┤ (e.g., Schema Registry Reject) │
  │                           │                                │
  ├─► Check Retry Count       │                                │
  │   retryCount + 1 >= MAX   │                                │
  │                           │                                │
  ├─► DeadLetterHandler ──────┼───────────────────────────────►│
  │   REQUIRES_NEW            │                                │
  │                           │                                │
  │                           │                                ├─► INSERT INTO outbox_dead_letter
  │                           │                                │     (Copy Event-10 payload & stacktrace)
  │                           │                                │
  │                           │                                ├─► DELETE FROM outbox_event
  │                           │                                │     WHERE id = :event10Id
  │                           │                                │
  │                           │                                ├─► UPDATE outbox_event
  │                           │                                │     SET next_retry_at = '9999-12-31'
  │                           │                                │     WHERE aggregate_id = 'Order-99'
  │                           │                                │   (Quarantines trailing events)
  │                           │                                │
  │                           │                                └─► COMMIT TX
  ▼                           ▼                                ▼
```

1. Event 10 encounters a terminal or unrecoverable error during Kafka dispatch.
2. If `retryCount + 1 >= maxRetries`, `DeadLetterHandler` activates an atomic `REQUIRES_NEW` transaction:
   * **Persist Dead Letter:** Copies event data, error message, and full stack trace into `outbox_dead_letter`.
   * **Evict Poison Pill:** Deletes Event 10 from `outbox_event` so it does not block unrelated traffic.
   * **Quarantine Aggregate:** Updates all remaining pending records with the same `aggregateId` to `next_retry_at = '9999-12-31 23:59:59Z'`.
3. The virtual bucket worker immediately continues processing events for other, healthy aggregates in the same bucket.

---

## 7. Deployment View

### 7.1 Infrastructure & Kubernetes Topology

The following diagram shows the deployment of microservice pods on Kubernetes, with the outbox extension coordinating over the database and streaming into Kafka:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Kubernetes Microservice Cluster                       │
│                                                                                 │
│   ┌───────────────────────────┐               ┌───────────────────────────┐     │
│   │   Service Pod 1           │               │   Service Pod 2           │     │
│   │  ┌─────────────────────┐  │               │  ┌─────────────────────┐  │     │
│   │  │ WorkloadAwarePoller │  │               │  │ WorkloadAwarePoller │  │     │
│   │  └──────────┬──────────┘  │               │  └──────────┬──────────┘  │     │
│   │             │             │               │             │             │     │
│   │      Leases Buckets       │               │      Leases Buckets       │     │
│   │       0, 1, 2, ...        │               │       3, 4, 5, ...        │     │
│   └─────────────┼─────────────┘               └─────────────┼─────────────┘     │
└─────────────────┼───────────────────────────────────────────┼───────────────────┘
                  │                                           │
                  ▼                                           ▼
      ┌───────────────────────────────────────────────────────────┐
      │          Relational Database Cluster (PostgreSQL / ...)   │
      │                                                           │
      │   ┌───────────────────────────┐ ┌──────────────────────┐  │
      │   │ Primary Application Pool  │ │ Dedicated Lock Pool  │  │
      │   │ - outbox_event            │ │ - outbox_lock        │  │
      │   │ - outbox_dead_letter      │ │   (Pre-seeded M=1024)│  │
      │   └───────────────────────────┘ └──────────────────────┘  │
      └───────────────────────────────────────────────────────────┘
                                    │
                                    │ (Kafka Dispatches)
                                    ▼
      ┌───────────────────────────────────────────────────────────┐
      │                   Apache Kafka Cluster                    │
      │                                                           │
      │      Topic: domain-events (Partitions 0, 1, 2, ... P-1)   │
      │      (Messages partitioned strictly by aggregateId key)   │
      └───────────────────────────────────────────────────────────┘
```

### 7.2 Connection Pool Bulkheading Configuration

To prevent high-volume business transactions from starving bucket lease heartbeats, the runtime configuration establishes two distinct Agroal connection pools:

```properties
# 1. Primary Application DataSource (Business Tables & Outbox Events)
quarkus.datasource.db-kind=postgresql
quarkus.datasource.jdbc.max-size=50
quarkus.datasource.jdbc.min-size=10

# 2. Dedicated Bulkhead Lock DataSource (Heartbeats & CAS Leases Only)
quarkus.datasource."outbox-lock".db-kind=postgresql
quarkus.datasource."outbox-lock".jdbc.max-size=10
quarkus.datasource."outbox-lock".jdbc.min-size=2
quarkus.datasource."outbox-lock".jdbc.acquisition-timeout=3s
```

---

## 8. Cross-Cutting Concepts

### 8.1 Domain Persistence Model

```
┌──────────────────────────────────────┐       ┌──────────────────────────────────────┐
│             OutboxEvent              │       │              OutboxLock              │
├──────────────────────────────────────┤       ├──────────────────────────────────────┤
│ id: UUID [PK, UUIDv7]                │       │ bucket_id: Integer [PK]              │
│ generation: Integer                  │       │ locked_by: String                    │
│ aggregate_id: String                 │       │ expires_at: Instant                  │
│ aggregate_type: String               │       └──────────────────────────────────────┘
│ bucket_id: Integer                   │
│ destination_topic: String            │       ┌──────────────────────────────────────┐
│ payload: String [@Lob]               │       │           OutboxDeadLetter           │
│ created_at: Instant                  │       ├──────────────────────────────────────┤
│ retry_count: Integer                 │       │ id: UUID [PK, UUIDv7]                │
│ next_retry_at: Instant               │       │ generation: Integer                  │
│ last_error: String                   │       │ aggregate_id: String                 │
└──────────────────────────────────────┘       │ aggregate_type: String               │
                                               │ bucket_id: Integer                   │
                                               │ destination_topic: String            │
                                               │ payload: String [@Lob]               │
                                               │ failed_at: Instant                   │
                                               │ retry_count: Integer                 │
                                               │ exception_message: String            │
                                               │ stack_trace: String [@Lob]           │
                                               │ status: String                       │
                                               └──────────────────────────────────────┘
```

#### Indexing Strategy
To optimize high-throughput batch drains and boundary watermark estimations across generation topologies, the underlying table maintains these indexes:
```sql
CREATE INDEX idx_outbox_gen_bucket_fifo ON outbox_event (generation ASC, bucket_id ASC, next_retry_at ASC, id ASC);
CREATE INDEX idx_outbox_aggregate ON outbox_event (aggregate_id);
```

### 8.2 Workload-Aware Fairness & Lag Estimation

Rather than executing a CPU-intensive `COUNT(*)` across large outbox tables, backlog depth is evaluated using $O(1)$ composite index boundary lookups:

```sql
SELECT 
    bucket_id, 
    (MAX(id) - MIN(id) + 1) AS estimated_lag
FROM outbox_event 
WHERE generation = :activeGeneration AND next_retry_at <= :now
GROUP BY bucket_id 
ORDER BY estimated_lag DESC;
```

Pods iterate through candidate buckets and greedily acquire locks until the sum of claimed lag reaches their processing budget ($C = 500$ messages).

### 8.3 Concurrency & Threading Guardrails

* **Virtual Thread Isolation:** Background heartbeat renewal loops are offloaded to Java 21 Virtual Threads (`Thread.ofVirtual()`), preventing operating system platform thread starvation.
* **Cooperative Fail-Fast Deadline Buffer:** Before initiating network transmission of each record, workers verify remaining lease time:
  ```java
  if (leaseTimeRemaining.compareTo(Duration.ofSeconds(2)) < 0) {
      log.warnf("Lease safety deadline (< 2000ms) reached for bucket %d. Yielding batch to prevent split-brain race.", bucketId);
      break;
  }
  ```
* **Vert.x Event Loop Protection:** Synchronous methods defend against blocking reactive threads:
  ```java
  if (!io.quarkus.runtime.configuration.ConfigUtils.isMainThread() 
      && io.vertx.core.Context.isOnEventLoopThread()) {
      throw new IllegalStateException("Blocking outbox invocation detected on Vert.x event-loop!");
  }
  ```

### 8.4 Kafka Dispatch & CloudEvents v1.0 Standards

Dispatched Kafka messages are formatted according to the **CNCF CloudEvents v1.0 Binary Content Mode**. Message headers carry event metadata, leaving the byte payload clean and free of proprietary envelopes:

| CloudEvent Header | Type | Value Source | Purpose |
| :--- | :--- | :--- | :--- |
| `ce_specversion` | String | `"1.0"` | CloudEvents specification version. |
| `ce_id` | String | `event.id.toString()` | Persistent UUIDv7 idempotency key for downstream deduplication. |
| `ce_type` | String | `event.aggregateType` | Event classifier for consumer filtering. |
| `ce_source` | String | Configured URI (e.g., `/orders`) | Originating service namespace. |
| `ce_time` | String | RFC 3339 formatted `Instant` | Event persistence timestamp. |
| `content-type` | String | `"application/json"` | Payload format descriptor. |
| `x-outbox-bucket` | String | `String.valueOf(event.bucketId)` | Diagnostic virtual shard identifier. |
| `x-outbox-generation`| String | `String.valueOf(event.generation)` | Diagnostic topology generation marker. |

**Partition Key Guarantee:** Every record sets its Kafka message key strictly to `aggregateId`. Kafka's default murmur2 partitioner guarantees all events for a given aggregate land on the exact same Kafka partition in sequence.

### 8.5 Observability & Management

* **Micrometer Metrics:**
  * `outbox.aggregates.quarantined`: Gauge tracking the active count of frozen aggregates.
  * `outbox.dispatch.duration`: Timer recording the execution time of the 3-phase sandwich loop.
  * `outbox.dead_letter.total`: Counter recording dead-letter events, tagged by `aggregate_type` and `reason`.
* **MicroProfile Health Checks:** Reports `@Readiness` as `UP`, embedding diagnostic metadata regarding active quarantined items.
* **Administrative REST API (`/q/outbox-kafka/admin`):**
  * `GET /q/outbox-kafka/admin/quarantined`: Lists all frozen aggregates and pending event counts.
  * `GET /q/outbox-kafka/admin/quarantined/{id}/events`: Returns dead-letter details and trailing frozen events.
  * `POST /q/outbox-kafka/admin/quarantined/{id}/replay`: Resets `next_retry_at = Instant.now()`.
  * `POST /q/outbox-kafka/admin/quarantined/{id}/skip`: Marks dead record `SKIPPED` and unfreezes trailing events.
  * `DELETE /q/outbox-kafka/admin/quarantined/{id}`: Removes all pending and dead-letter events for the aggregate.
  * `POST /q/outbox-kafka/admin/drain`: Signals on-demand graceful queue drainage prior to maintenance.

### 8.6 Summary Matrix of Architectural Invariants for Coding Agents

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

## 9. Architectural Decision Records (ADRs)

### ADR-001: Native KafkaProducer vs. SmallRye Reactive Messaging

* **Status:** Accepted

#### Context
SmallRye Reactive Messaging uses static channel-to-topic bindings defined at build time. An outbox extension requires dynamic runtime destination routing, low-allocation batch iterations, and manual acknowledgment tracking to coordinate the "Short-Transaction Sandwich". Furthermore, reactive channels introduce intermediate buffer queues and multi-layered reactive streams allocations that conflict with our $\le 512\text{ bytes}$ per record allocation budget.

#### Decision
Depend directly on `io.quarkus:quarkus-kafka-client` and use the low-level `org.apache.kafka.clients.producer.KafkaProducer` API wrapped in an application-scoped dispatcher bean.

#### Consequences

##### Positive
- Native batch pipelining directly into Kafka's internal `RecordAccumulator`.
- Dynamic destination topics per outbox row without needing statically configured channels.
- Full control over synchronous or asynchronous acknowledgment collection across batch boundaries.
- Eliminates channel boilerplate, reactive stream wrapping overhead, and unneeded dependencies.

##### Negative
- Requires custom manual error tracking, retry backoff calculation, and callback error propagation instead of framework-provided channel fault-tolerance annotations.
- Manual lifecycle management (producer initialization, resource cleanup on shutdown).

#### Non-Negotiable Invariants
- Runtime dispatch code must NEVER use SmallRye Reactive Messaging channels or `@Incoming`/`@Outgoing` annotations.
- The `KafkaProducer` instance must be reused as an application-scoped singleton, thread-safe across virtual threads.
- Message acknowledgments must be tracked in-memory to execute Phase 3 batch deletes.

### ADR-002: High-Cardinality Virtual Pre-Sharding ($M = 1024$ Default)

* **Status:** Accepted

#### Context
Tying database partitions or bucket IDs directly to physical Kafka partition counts creates data hazards: resizing Kafka partitions causes events for the same aggregate to hash to different buckets, breaking FIFO ordering. Furthermore, low bucket counts (e.g., 8 or 16) create hot-key contention where a single busy aggregate blocks or delays processing for unrelated entities.

#### Decision
Decouple physical messaging topology from persistence sharding by mapping aggregate keys across high-cardinality virtual buckets (default $M = 1024$). The bucket ID is calculated using `(MurmurHash3.hash32(aggregateId) & Integer.MAX_VALUE) % M`.

#### Consequences

##### Positive
- Completely decouples the persistent database schema from Kafka broker topology, partition counts, and application pod scaling.
- Reduces hot-key blast radius to $< 0.1\%$ per aggregate.
- Allows independent horizontal scaling of worker pods without re-partitioning the database or migrating data.

##### Negative
- Requires maintaining $M$ lock rows in the database (`outbox_lock`).
- Requires worker pods to lease multiple buckets (e.g., in ranges or chunks) and execute polling loops across assigned bucket partitions.

#### Non-Negotiable Invariants
- The hashing function MUST be positive (masking sign bit: `hash & Integer.MAX_VALUE`).
- Virtual bucket count $M$ must default to 1024.
- Events for the same `aggregateId` within the same generation MUST deterministically map to the exact same `bucket_id`.

### ADR-003: Pre-Seeded Lock Rows over Dynamic SQL Upserts

* **Status:** Accepted

#### Context
Standard Jakarta Persistence (JPA / JPQL) lacks a portable cross-database `UPSERT` / `MERGE` construct. Proprietary syntax (`ON CONFLICT DO UPDATE`, `ON DUPLICATE KEY UPDATE`, `MERGE INTO`) breaks database portability across PostgreSQL, MySQL, MariaDB, Oracle, and MS SQL Server. Dynamic insertion during lock acquisition also risks insertion race conditions and transaction deadlocks.

#### Decision
Pre-seed the `outbox_lock` table with rows $0 \dots M-1$ during schema initialization or migration. Lock acquisition and renewal operate strictly via portable atomic JPQL Compare-And-Swap (CAS) `UPDATE` queries against pre-existing rows.

#### Consequences

##### Positive
- Lock acquisition and release are expressed as portable ANSI JPQL bulk `UPDATE` statements using Compare-And-Swap logic (`UPDATE OutboxLock l SET l.lockedBy = :podId, l.leaseUntil = :newLease WHERE l.bucketId = :bucketId AND (l.leaseUntil < :now OR l.lockedBy = :podId)`).
- Guarantees 100% portability across all SQL dialects supported by Hibernate ORM without native SQL dialects.
- Eliminates row-level insertion lock contention and duplicate key conflicts.

##### Negative
- Requires an initialization step (Flyway/Liquibase migration or extension auto-seeder) to insert $M$ rows before worker polling begins.
- Schema change required if virtual bucket count $M$ is reconfigured.

#### Non-Negotiable Invariants
- Dynamic `INSERT` or native vendor-specific `UPSERT` queries are strictly forbidden in lock acquisition routines.
- Lock acquisition and renewal MUST execute as atomic JPQL CAS updates against existing `bucket_id` rows.

### ADR-004: Hibernate StatelessSession for Outbox Processing

* **Status:** Accepted

#### Context
Polling, hydrating, and deleting thousands of events using a standard JPA `EntityManager` introduces dirty-checking overhead, persistence context hydration lag, first-level cache accumulation, and severe garbage collection pressure. This violates our SLA of $\le 512\text{ bytes}$ heap allocation per record and causes memory bloat.

#### Decision
Execute batch polling, dead-letter writes, and batch deletion exclusively via Hibernate's `StatelessSession` using direct Java 21 constructor record projections (`OutboxBatchRecord`).

#### Consequences

##### Positive
- Completely bypasses first-level cache, entity management snapshots, and automatic dirty-checking routines.
- Executes direct JDBC-level queries with type-safe JPA mappings.
- Enables direct constructor projection into lightweight immutable Java 21 records (`OutboxBatchRecord`), achieving zero entity hydration overhead and meeting our $\le 512\text{ bytes}$ allocation budget.

##### Negative
- No automatic dirty checking or cascade persistence; all database operations (inserts, updates, deletes) must be explicitly invoked on the `StatelessSession`.
- Requires careful lifecycle management of `StatelessSession` instances within programmatic transaction boundaries.

#### Non-Negotiable Invariants
- Full JPA managed entities (`OutboxEvent`) must NEVER be hydrated in polling or dispatch worker loops.
- All outbox reads in the worker loop must use direct record projections (`OutboxBatchRecord`) via `StatelessSession`.
- `StatelessSession` instances must be opened and closed strictly within their respective short transaction boundaries (Phase 1 and Phase 3).

### ADR-005: Dedicated Connection Pool Bulkhead for Locks and Heartbeats

* **Status:** Accepted

#### Context
Heavy domain business transactions, long-running batch queries, or transient database slowdowns can easily exhaust the primary Agroal connection pool. If an outbox worker pod cannot acquire a database connection to renew its bucket lease before $T_{\text{lease}}$ expires, its lease is stolen by a peer pod, resulting in split-brain execution and transient duplicate dispatches.

#### Decision
Physically isolate lock acquisition and heartbeat renewals into a dedicated Agroal named data source pool (`quarkus.datasource."outbox-lock"`).

#### Consequences

##### Positive
- Guarantees that heartbeat renewals succeed within millisecond deadlines, completely isolated from application transaction volume and connection pool starvation.
- Provides predictable lease timing and prevents premature lease loss caused by domain traffic spikes.

##### Negative
- Requires a small additional connection allocation (typically 2-5 connections per pod) reserved exclusively for lock maintenance.
- Requires configuring and managing two Agroal DataSource instances in the Quarkus runtime.

#### Non-Negotiable Invariants
- Lock acquisition and heartbeat supervisor threads must NEVER borrow connections from the default/application Agroal connection pool.
- All lease renewals must execute against the dedicated `outbox-lock` data source pool.

### ADR-006: Aggregate-Level Quarantining over Bucket-Level Halting

* **Status:** Accepted

#### Context
When an unprocessable record (poison pill, schema incompatibility, topic authorization failure) fails repeatedly, halting the entire virtual bucket blocks hundreds of unrelated, healthy aggregates sharing that bucket. Conversely, dropping the event or skipping it while continuing to dispatch subsequent events for that same aggregate corrupts domain state machines by violating FIFO ordering.

#### Decision
Implement **Aggregate-Level Quarantining**. When an event reaches `maxRetries`, it is copied to `OutboxDeadLetter`, removed from `OutboxEvent`, and all trailing events for that specific `aggregateId` are frozen in place by setting `next_retry_at = '9999-12-31'`. The worker loop continues dispatching all other healthy aggregates in the virtual bucket.

#### Consequences

##### Positive
- Unrelated aggregates in the same virtual bucket continue flowing with zero interruption (high bucket availability).
- Strict FIFO ordering is preserved for the affected aggregate: no trailing events can be dispatched before the dead-letter issue is resolved.
- Administrative endpoints (`/q/outbox-kafka/admin/quarantined`) allow operators to inspect, replay, or skip frozen aggregates.

##### Negative
- Requires additional administrative REST APIs and operational tooling to inspect and unfreeze quarantined aggregates.
- Trailing events for the quarantined aggregate accumulate in the outbox table until operator intervention.

#### Non-Negotiable Invariants
- Never drop a poison pill event while allowing subsequent events for the same aggregate to be dispatched.
- Poisoned events exceeding `maxRetries` MUST be written to `OutboxDeadLetter` and trailing events for that aggregate MUST be frozen (`next_retry_at = '9999-12-31'`).
- The worker polling filter must exclude frozen records (`WHERE next_retry_at <= :now`).

### ADR-007: Persistent UUIDv7 Identifiers & Consumer-Side Idempotency Contract

* **Status:** Accepted

#### Context
In distributed systems lacking XA two-phase commit between RDBMS and Kafka, Stop-The-World (STW) GC pauses or network stalls create Time-of-Check to Time-of-Use (TOCTOU) races: a pod pauses between its local lock check and network socket flush, its lease is stolen by a peer, and upon resumption it flushes stale duplicate records to Kafka. In-memory flag checks cannot prevent socket flushes once bytes reach OS buffers.

#### Decision
1. Mandate **at-least-once delivery with consumer-side idempotency**.
2. Generate entity primary key `id` as a time-ordered **UUIDv7** (RFC 9562) upon event creation and map it 1:1 to CloudEvents header `ce_id`. Dynamic random UUID generation in dispatch loops is strictly prohibited.
3. Enforce a cooperative worker buffer: workers must check `if (leaseTimeRemaining < Duration.ofSeconds(2))` before each record dispatch, aborting active batches before lease expiration.

#### Consequences

##### Positive
- UUIDv7 preserves database B-tree index locality and chronological insertion performance while providing globally unique, immutable identifiers.
- Downstream consumers reliably deduplicate retried or stale messages using `ce_id`.
- The 2-second cooperative worker buffer drastically reduces duplicate dispatch occurrences during near-expiry lease conditions.

##### Negative
- Downstream consumers must implement deduplication (e.g., maintaining an idempotent message store keyed by `ce_id`).
- Requires UUIDv7 generation logic in the event factory/entity lifecycle.

#### Non-Negotiable Invariants
- `OutboxEvent.id` must be generated as UUIDv7 upon creation and persisted in the database.
- Dynamic random UUID generation (`UUID.randomUUID()`) inside dispatch loops or header mappers is strictly forbidden.
- The CloudEvents `ce_id` header MUST match the persisted `OutboxEvent.id`.
- Workers must enforce the cooperative $\ge 2\text{s}$ lease buffer prior to initiating network sends.

### ADR-008: Online Generation/Epoch Gating for Zero-Downtime Dynamic Resharding

* **Status:** Accepted

#### Context
Dynamic bucket resizing causes events for the same aggregate to hash to different buckets, which peer pods can lease concurrently, causing out-of-order dispatches ("The Hash Reshuffling Catastrophe"). Halting writes or forcing offline maintenance windows impairs high-availability production workloads.

#### Decision
Adopt **Online Generation/Epoch Gating**:
- All outbox records persist a `generation` column with composite index `(generation ASC, bucket_id ASC, next_retry_at ASC, id ASC)`.
- When reconfiguring virtual shards, operators increment `quarkus.outbox.generation` and update `quarkus.outbox.virtual-shards`.
- Application transactions immediately write subsequent events using the new generation and new bucket count without downtime or transaction pausing.
- Workers enforce a poller barrier: `SELECT MIN(generation) FROM OutboxEvent`. Workers exclusively process older generation events until drained to 0, whereupon processing naturally advances to the new generation.

#### Consequences

##### Positive
- Delivers continuous zero-downtime scaling with strict per-aggregate FIFO guarantees and zero cross-generation hash collisions.
- No database maintenance window, application stop, or read/write pausing required during resharding.
- Simple, deterministic progression governed by database state.

##### Negative
- Requires a composite database index including `generation`.
- Requires an extra lightweight query (`SELECT MIN(generation)`) at the start of polling cycles when multiple generations might exist.

#### Non-Negotiable Invariants
- Application writes must always attach the currently active `generation` configuration to new `OutboxEvent` records.
- Workers must NEVER poll or dispatch events from generation $G + 1$ while any unprocessed events remain in generation $G$.
- Resharding must always increment the generation monotonically.

---

## 10. Quality Requirements

### 10.1 Quality Tree

```
Quality Requirements
├── Performance & Latency
│   ├── High Throughput (≥ 10,000 msgs/sec per pod) [QG-1]
│   ├── Low Transit Latency (p50 ≤ 15ms, p95 ≤ 50ms, p99 ≤ 100ms) [QG-2]
│   ├── Zero DB Connection Hold Time in Phase 2 (0ms) [QG-3]
│   └── Fast Backlog Estimation (p99 ≤ 2ms boundary scan) [QG-4]
├── Memory & Allocation Efficiency
│   ├── Low Allocation Rate (≤ 512 bytes / record) [QG-5]
│   ├── Zero Entity Hydration Churn (StatelessSession record projection) [QG-6]
│   ├── Low GC Pause Budget (p99.9 ≤ 10ms ZGC, max STW < 50ms G1) [QG-7]
│   ├── Compact Extension Heap Overhead (≤ 32 MB) [QG-8]
│   ├── Lean GraalVM Native Image RSS (≤ 64 MB idle, ≤ 96 MB peak) [QG-9]
│   ├── Virtual Thread Stack Overhead (≤ 1 KB per supervisor) [QG-10]
│   └── Bounded In-Flight Queue Memory (≤ 5 MB per pod) [QG-11]
├── Reliability & Correctness
│   ├── Strict Aggregate FIFO Order (0 out-of-sequence anomalies) [QG-12]
│   ├── Replay Deduplication via Persistent UUIDv7 ce_id (100% deduplicable) [QG-13]
│   ├── Bulkhead Isolation Under Starvation (100% heartbeat success) [QG-14]
│   └── Split-Brain Immunity (Fail-fast lease revocation & 2s deadline buffer)
├── Portability
│   ├── 100% ANSI JPQL Compliance
│   └── Multi-DB Compatibility (Postgres, MySQL, Oracle, SQL Server, H2)
└── Operability
    ├── CNCF CloudEvents v1.0 Binary Compliance
    ├── Micrometer Metrics & MicroProfile Health Checks
    └── Operator Self-Service REST API (/q/outbox-kafka/admin)
```

### 10.2 Quality Scenarios

#### Scenario QS-1: Pod Freeze / Stop-The-World GC Pause (Split-Brain Prevention)
* **Stimulus:** Pod A holds the lease for Bucket 42 and encounters a 45-second GC pause or network stall.
* **Response:** Pod A's lease expires at $T = 10\text{s}$. Pod B detects the expired lease in `outbox_lock` and claims Bucket 42. When Pod A unfreezes, its supervisor thread detects that the renewal deadline ($\frac{2}{3} \times T_{\text{lease}}$) has passed, sets `leaseLost = true`, and interrupts the worker. Pod A's worker terminates execution before dispatching its next record due to the $T_{\text{remaining}} < 2000\text{ms}$ buffer check.
* **Measurable Metric:** Even if late-flushed OS socket buffers deliver duplicate records, both Pod A and Pod B transmit identical persistent UUIDv7 `ce_id` headers. The downstream consumer idempotency store drops $100\%$ of duplicates without partition sequence corruption (QG-13).

#### Scenario QS-2: Database Connection Pool Exhaustion Under Peak Load
* **Stimulus:** Primary application business transactions flood the default Agroal pool, consuming all 50 available connections.
* **Response:** Background virtual thread heartbeat supervisors continue renewing bucket leases via the physically isolated `quarkus.datasource."outbox-lock"` connection pool.
* **Measurable Metric:** $100\%$ of bucket lease heartbeats succeed within $T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$ ($< 10\text{ms}$ latency); zero false-positive lease drops occur while the primary application pool is 100% exhausted (QG-14).

#### Scenario QS-3: High-Throughput Batch Pipeline Under Steady Load
* **Stimulus:** Upstream business services produce continuous events at a rate of 10,000 events/second with a batch size of 100 and 1KB average payload size.
* **Response:** Outbox worker drains candidate buckets using pipelined Short-Transaction Sandwiches, streaming records directly into Kafka's `RecordAccumulator`.
* **Measurable Metric:** Sustained throughput $\ge 10{,}000\text{ msgs/sec}$ per pod (QG-1). End-to-end transit latency from DB commit to Kafka broker ACK remains $\text{p50} \le 15\text{ms}$, $\text{p95} \le 50\text{ms}$, and $\text{p99} \le 100\text{ms}$ (QG-2).

#### Scenario QS-4: Zero Entity Hydration & Low-Allocation Memory Benchmark
* **Stimulus:** A worker pod processes 1,000,000 outbox records continuously.
* **Response:** Hibernate `StatelessSession` queries project database tuples directly into Java 21 `OutboxBatchRecord` instances without allocating managed entity instances, dirty-checking descriptors, or first-level persistence context maps.
* **Measurable Metric:** Heap allocation rate remains $\le 512\text{ bytes}$ per record (QG-5); zero JPA entity instances hydrated in poll loops (QG-6); GC pause time remains $\text{p99.9} \le 10\text{ms}$ under ZGC/Shenandoah with max STW pause $< 50\text{ms}$ under G1 GC (QG-7).

#### Scenario QS-5: Compact Native Image Footprint
* **Stimulus:** Microservice is compiled to a GraalVM/Mandrel native Linux binary and deployed in a resource-constrained container (1 vCPU, 512MB RAM).
* **Response:** The application boots in $< 50\text{ms}$ and processes production outbox traffic.
* **Measurable Metric:** Container Resident Set Size (`VmRSS`) remains $\le 64\text{ MB}$ at baseline and $\le 96\text{ MB}$ at peak throughput of 10,000 msgs/sec (QG-9). Total extension JVM heap overhead in standard JVM mode is $\le 32\text{ MB}$ (QG-8).

#### Scenario QS-6: Short-Transaction Sandwich Zero-Connection Transit Guarantee
* **Stimulus:** 2,000ms latency is injected into the Kafka broker dispatch transport layer.
* **Response:** Outbox worker retrieves a batch of 50 records in Phase 1 (select), immediately commits the transaction, and returns the Agroal database connection to the pool before initiating Kafka network send.
* **Measurable Metric:** Active database connections held from the primary Agroal pool during Phase 2 Kafka network transmission is strictly **$0\text{ms}$ / 0 connections** (QG-3). Phase 1 select duration $\le 3\text{ms}$ (p99); Phase 3 bulk delete duration $\le 5\text{ms}$ (p99).

---

## 11. Risks and Technical Debt

| Risk / Debt | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **Sparse Bucket Overhead** | Low-traffic systems with few events spread across 1024 shards incur multiple single-item transactions. | The workload-aware poller limits scans to active buckets with pending lag $> 0$, preventing empty bucket poll queries. |
| **Cross-Topic Ordering Limits** | While events are dispatched in database order, Kafka provides no sequence guarantees across different topics. | The documentation specifies that strict FIFO ordering applies per partition within a single topic; cross-topic ordering requires consumer-side sequencing. |
| **System Clock Skew** | Out-of-sync system clocks across Kubernetes worker nodes could cause premature lease takeovers. | Production deployments must run Network Time Protocol (NTP) synchronization daemons across all Kubernetes nodes. Lease expiration logic includes defensive safety margins ($\frac{2}{3} \times T_{\text{lease}}$). |

---

## 12. Glossary

| Term | Definition |
| :--- | :--- |
| **Aggregate ID** | The unique identifier of a business entity (e.g., `order-12345`). Used as both the bucket hashing seed and the Kafka message partition key. |
| **CloudEvents** | A CNCF specification for describing event data in common formats, facilitating event routing and processing across distributed systems. |
| **Compare-And-Swap (CAS)** | An atomic conditional update pattern used in SQL (`UPDATE ... WHERE locked_by = :me OR expires_at < :now`) to avoid race conditions. |
| **Greedy Multi-Fit** | An approximation algorithm that allocates virtual buckets to worker pods based on their estimated pending message count until a processing budget is met. |
| **Poison Pill** | A malformed or unprocessable outbox record that consistently fails dispatch (e.g., schema registry failure or serialization bug). |
| **Short-Transaction Sandwich** | An architectural pattern that separates database read operations, external network dispatch, and database delete operations into distinct phases to avoid holding database transactions open across the network. |
| **StatelessSession** | A lightweight Hibernate session interface that bypasses dirty checking and first-level caching, achieving near-JDBC speeds. |
| **Virtual Bucketing** | A partitioning technique that maps an arbitrary key space into a fixed number ($M$) of virtual logical queues, each leased exclusively by at most one worker pod. |
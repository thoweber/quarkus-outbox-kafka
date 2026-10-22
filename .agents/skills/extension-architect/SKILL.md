---
name: extension-architect
description: Enforces Quarkus extension architecture invariants, module boundaries, build-time augmentations, low-allocation design, and the Short-Transaction Sandwich.
version: 1.0.0
---

# Extension Architect Skill (`extension-architect`)

This skill governs software architecture, Quarkus extension boundaries, build-time augmentations, database transaction lifecycle, and low-allocation design across **quarkus-outbox-kafka**.

> [!IMPORTANT]
> **Mandatory Architecture Activation:** The `extension-architect` skill **MUST** be activated during story planning, design reviews, feature implementation, and refactoring to guarantee compliance with Quarkus extension rules and the Outbox Invariants Matrix.

---

## 1. Quarkus Extension Module Boundaries

The repository is organized into three strictly segregated modules:

```text
quarkus-outbox-kafka/
├── runtime/            # CDI beans, JPA entities, lock providers, Kafka dispatcher
├── deployment/         # Build-time augmentations, @BuildStep processors, bytecode generation
└── integration-tests/  # Dev Services integration tests (PostgreSQL + Kafka containers)
```

### Boundary Rules
1. **Never Import Deployment in Runtime:** Classes in `runtime/` must NEVER import `io.quarkus.deployment.*`. Violation breaks compilation and packaging.
2. **No Runtime Business Logic in Deployment:** Classes in `deployment/` execute strictly inside the Quarkus build augmentation phase (Maven compile / GraalVM analysis). They must not contain runtime dispatch loops.
3. **Hermetic Build-Time Unit Tests:** Unit tests in `deployment/src/test` verify build steps and configuration augmentations using `QuarkusUnitTest`. They must execute in $< 3\text{s}$ without starting Docker containers (`quarkus.kafka.devservices.enabled=false`).

---

## 2. Build-Time Augmentation (`QuarkusOutboxKafkaProcessor`)

Build-time processing is centralized in `deployment/src/main/java/.../deployment/QuarkusOutboxKafkaProcessor.java`:

* **Feature Registration:** Produces `FeatureBuildItem("outbox-kafka")`.
* **JPA Model Indexing:** Registers `OutboxEvent`, `OutboxDeadLetter`, and `OutboxLock` entities via `AdditionalJpaModelBuildItem` to ensure Hibernate ORM discovers them without explicit `persistence.xml`.
* **CDI Bean Registration:** Registers dispatcher, polling workers, lock managers, and REST management endpoints via `AdditionalBeanBuildItem.unremovableOf(...)`.
* **Build-Time Config Validation:** Validates `quarkus.outbox.virtual-shards` (power-of-two recommendation or positive integer), `quarkus.outbox.generation` ($\ge 0$), and fail-fast lease parameters at build time.

---

## 3. The Short-Transaction Sandwich Lifecycle

To prevent database connection starvation and thread pool exhaustion, workers strictly adhere to the 3-phase lifecycle:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 1: Short Read (Transaction 1)                                         │
│ - Open StatelessSession via QuarkusTransaction.requiringNew().call(...)     │
│ - Query batch of OutboxBatchRecord projections                              │
│ - Commit & Close transaction immediately (DB connection returned to pool)  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 2: Network Transit (Zero Active DB Connections)                       │
│ - DB Connection Hold Time == 0 ms                                          │
│ - Check remaining lease duration: if (remaining < 2000ms) break loop        │
│ - Dispatch ProducerRecord to Kafka (key = aggregateId, ce_id = UUIDv7)     │
│ - Collect RecordMetadata acks & exceptions in memory                        │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 3: Short Write (Transaction 2)                                        │
│ - Open StatelessSession via QuarkusTransaction.requiringNew().call(...)     │
│ - Batch DELETE successful records (DELETE WHERE id IN (:ackedIds))          │
│ - If poison pill detected: write to OutboxDeadLetter & freeze aggregate     │
│ - Commit & Close transaction immediately                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Transaction Boundary Rules
* **No Open Connections During I/O:** NEVER hold an open database connection or transaction during Kafka network operations.
* **No Self-Invocation:** Never call `@Transactional` methods on `this` within the same class; self-invocation bypasses ArC client proxies. Use `QuarkusTransaction.requiringNew()`.

---

## 4. Zero-Hydration & Low-Allocation Budget ($\le 512\text{B}$/Record)

To satisfy **QG-5** and **QG-6** from `.agents/config.json`:
1. **Zero Entity Hydration:** NEVER query or hydrate full `OutboxEvent` managed entities in polling loops. Always use Hibernate `StatelessSession` with direct constructor record projections (`OutboxBatchRecord`).
2. **Static CloudEvents Headers:** CloudEvents header keys must be defined as `static final byte[]` constants:
   ```java
   private static final Header CE_SPEC_VERSION = new RecordHeader("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));
   ```
3. **No Dynamic String Concatenation:** Avoid `String.format()` or string concatenation in dispatch loops.
4. **Positive Murmur3 Hash:** Bucket calculation must mask the sign bit:
   ```java
   int bucketId = (MurmurHash3.hash32(aggregateId) & Integer.MAX_VALUE) % totalBuckets;
   ```

---

## 5. Bulkhead Concurrency & Lock Leases

1. **Dedicated Agroal DataSource Bulkhead:** All lock CAS updates and heartbeats must execute against `quarkus.datasource."outbox-lock"`. Never borrow connections from the default/application pool.
2. **Virtual Thread Supervisors:** Background heartbeat supervisor loops must run on Java 21 Virtual Threads (`Thread.ofVirtual().name("outbox-heartbeat-", bucketId).start(...)`).
3. **Timing Invariants:**
   - Heartbeat renewal interval: $T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$.
   - Fail-fast threshold: If renewal fails past $\frac{2}{3} \times T_{\text{lease}}$, supervisor sets `leaseLost = true` and interrupts the worker thread.
   - Cooperative worker buffer: Workers must check `remainingLeaseDuration >= 2000ms` prior to each dispatch.

---

## 6. Identifiers & Online Generation Gating

1. **Persistent UUIDv7:** Entity primary key `id` must be generated as a time-ordered UUIDv7 (RFC 9562) upon event creation and mapped 1:1 to CloudEvents `ce_id`. Dynamic generation in memory (`UUID.randomUUID()`) is forbidden.
2. **Online Generation Gating:** Dynamic bucket resizing uses a persistent `generation` column with composite index `(generation ASC, bucket_id ASC, next_retry_at ASC, id ASC)`. Workers enforce a poller barrier (`SELECT MIN(generation)`) and drain older generations completely before processing newer generations.

---

## 7. Architectural Invariants Checklist

When reviewing or generating code, ensure:
* [ ] No `io.quarkus.deployment.*` imports in `runtime/`.
* [ ] DB connection is strictly closed before calling `KafkaProducer.send(...)`.
* [ ] Polling queries use `StatelessSession` and return `OutboxBatchRecord`, not `OutboxEvent`.
* [ ] Header keys are pre-allocated `static final byte[]`.
* [ ] Lock queries run against the `outbox-lock` named data source.
* [ ] Murmur3 hash computation masks the sign bit (`& Integer.MAX_VALUE`).
* [ ] Poison pills freeze trailing events for that aggregate (`next_retry_at = '9999-12-31'`).

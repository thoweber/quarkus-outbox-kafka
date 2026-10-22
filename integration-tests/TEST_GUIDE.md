# Integration Test Guide: Quarkus Transactional Outbox

This document defines the automated verification strategy and test harness specifications for the `quarkus-outbox-kafka` extension.

All integration tests reside in `integration-tests/` and execute against real infrastructure using **Quarkus Dev Services** (PostgreSQL and Apache Kafka Testcontainers). **Mocking database pools or Kafka brokers in integration tests is strictly forbidden.**

---

## 1. Test Environment & Dev Services Configuration

The test environment leverages Quarkus Dev Services to spin up zero-config containers automatically during `./mvnw verify`:
* **PostgreSQL:** Backs both the application persistence pool and the dedicated `"outbox-lock"` Agroal bulkhead pool.
* **Apache Kafka:** Backs the native `KafkaProducer` message transport and consumer verification probes.

### Recommended Test Configuration (`integration-tests/src/main/resources/application.properties`)
```properties
# Enable Dev Services for Integration Testing
quarkus.datasource.devservices.enabled=true
quarkus.datasource.db-kind=postgresql

# Dedicated Lock Bulkhead DataSource
quarkus.datasource."outbox-lock".devservices.enabled=false
quarkus.datasource."outbox-lock".db-kind=postgresql
quarkus.datasource."outbox-lock".jdbc.url=${quarkus.datasource.jdbc.url}
quarkus.datasource."outbox-lock".username=${quarkus.datasource.username}
quarkus.datasource."outbox-lock".password=${quarkus.datasource.password}
quarkus.datasource."outbox-lock".jdbc.max-size=5
quarkus.datasource."outbox-lock".jdbc.min-size=2

# Kafka Dev Services
quarkus.kafka.devservices.enabled=true

# Outbox Runtime Configuration
quarkus.outbox.generation=1
quarkus.outbox.virtual-shards=1024
quarkus.outbox.poll-interval=500ms
quarkus.outbox.lock.lease-duration=5s
quarkus.outbox.pod.capacity=100
```

---

## 2. Core Invariant Test Suites

The test suite must implement four mandatory integration tests verifying the fundamental architectural invariants:

### 2.1 `PhantomProducerIT` (GC Pause & Split-Brain Simulation)
* **Goal:** Verify that a stalled worker pod whose lease is stolen by a peer cannot cause out-of-order dispatches or corrupt Kafka partition sequences upon resuming.
* **Failure Scenario:**
  1. Pod A leases Bucket 42 and reads event $E_1$ (Aggregate `Order-123`, UUIDv7 ID $U_1$).
  2. Suspend Pod A's bucket worker thread (simulating a 45-second GC pause or network stall).
  3. Wait for the lease duration ($T = 5\text{s}$) to expire in `outbox_lock`.
  4. Pod B detects the expired lease for Bucket 42, claims it, reads $E_1$ and $E_2$, dispatches both to Kafka with their persistent UUIDv7 `ce_id` headers, and deletes them from `outbox_event`.
  5. Resume Pod A's worker thread.
* **Assertions:**
  1. Pod A detects `leaseLost = true` (or its cooperative check $T_{\text{remaining}} < 2000\text{ms}$ fails) and halts batch processing.
  2. If late OS socket buffers flush $E_1$ from Pod A, the record carries the exact same persistent `ce_id` ($U_1$).
  3. The downstream Kafka consumer idempotency layer verifies that duplicate $E_1$ is safely discarded and that partition log sequence integrity is preserved.

### 2.2 `ShortTransactionSandwichIT` (Zero DB Connection Retention During I/O)
* **Goal:** Verify that zero database connections from the primary Agroal pool are held open while transmitting records over the network to Kafka brokers.
* **Failure Scenario:**
  1. Inject 2000ms network transit latency into the Kafka producer dispatch phase (e.g., via a delayed interceptor or proxy).
  2. Outbox worker begins dispatching a batch of 50 records to Kafka.
  3. During the 2000ms broker transit window, asynchronously inspect the Agroal primary DataSource metrics:
     ```java
     int activeConnections = agroalDataSource.getMetrics().activeCount();
     ```
* **Assertions:**
  1. Assert that `activeConnections == 0` throughout the entire broker wait phase.
  2. Confirm that database connections are borrowed strictly during Phase 1 (short select) and Phase 3 (bulk delete).

### 2.3 `AggregateQuarantineIT` (Poison Pill & Trailing Freeze)
* **Goal:** Verify that an unprocessable event (poison pill) is moved to DLQ, freezes trailing events for that aggregate, and does not block sibling aggregates in the same virtual bucket.
* **Failure Scenario:**
  1. Insert 5 events for `Order-123` and 5 events for `Order-456` into the same virtual bucket ($bucketId = 12$).
  2. Inject a simulated permanent delivery failure (e.g. topic authorization error or serialization failure) targeting event 2 of `Order-123`.
  3. The worker retries event 2 with exponential backoff until `retry_count >= dlq.max-retries`.
* **Assertions:**
  1. Event 2 moves to `outbox_dead_letter` with `status = 'DEAD'`.
  2. Trailing events 3, 4, 5 for `Order-123` have `next_retry_at` set to century-end (`9999-12-31 23:59:59Z`).
  3. Sibling events 1..5 for `Order-456` in the same bucket drain and publish to Kafka without delay.
  4. Call `POST /q/outbox-kafka/admin/quarantined/Order-123/replay`: verify `next_retry_at` resets and events resume.
  5. Call `POST /q/outbox-kafka/admin/quarantined/Order-123/skip`: verify DLQ record is marked `SKIPPED` and trailing events unfreeze.
  6. Call `DELETE /q/outbox-kafka/admin/quarantined/Order-123`: verify dead-letter and pending events are purged.

### 2.4 `OutboxConcurrencyIT` (Multi-Pod Contention & Strict FIFO Ordering)
* **Goal:** Stress-test multi-pod concurrent execution under high write load and verify that per-aggregate FIFO ordering is 100% preserved.
* **Failure Scenario:**
  1. Initialize `outbox_lock` with 1024 pre-seeded rows.
  2. Simulate 3 concurrent worker pods competing for bucket leases via greedy bin-packing.
  3. Concurrently insert 1,000 events spread across 50 distinct `aggregateId` keys.
* **Assertions:**
  1. Ensure no two worker pods ever hold the same bucket lock simultaneously.
  2. Consume messages from Kafka topic and verify that for every single `aggregateId`, events are received in strictly ascending sequence ($V_1 \to V_2 \to V_3 \dots$).
  3. Verify zero message loss, zero duplicate deliveries under normal conditions, and zero database deadlocks.

---

## 3. Test Execution Commands

```bash
# Execute JVM integration tests against Dev Services containers
./mvnw clean verify -pl integration-tests

# Execute a single integration test class
./mvnw verify -pl integration-tests -Dit.test=PhantomProducerIT

# Execute integration tests against GraalVM native binary
./mvnw verify -pl integration-tests -Dnative
```

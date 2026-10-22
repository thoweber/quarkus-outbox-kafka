# Distributed Systems Invariants & Failure-Mode Defenses

This rule document codifies the operational edge cases, failure scenarios, and defensive invariants established for the `quarkus-outbox-kafka` extension.

Any AI coding agent or engineer modifying `runtime/`, `deployment/`, or `integration-tests/` **MUST** adhere to the patterns and contracts defined herein.

---

## 1. Scenario 1: The Phantom Producer (GC Pause & Split-Brain Dispatch)

### Failure Mode
1. **Pod A** leases Bucket 4 and reads event $E_1$ (Aggregate `Order-123`, version 1).
2. Pod A verifies its local in-memory lock status (`isStillOwnedLocally() == true`).
3. Immediately before the TCP payload leaves the socket to the Kafka broker, Pod A hits a **45-second Stop-The-World (STW) JVM GC pause** or network stall.
4. Pod A's lease expires in `outbox_lock`.
5. **Pod B** claims the expired lease for Bucket 4, reads $E_1$ and $E_2$ from the database, successfully delivers both to Kafka, and deletes them from the database.
6. **Pod A wakes up.** Its pending OS socket buffer flushes. Pod A dispatches its stale copy of $E_1$ to Kafka.
7. **Broker Log Sequence:**
   $$\text{Partition Log: } [E_1] \longrightarrow [E_2] \longrightarrow [E_1 \ (\text{duplicate stale arrival})]$$

### Theoretical Reality
In distributed systems, checking local state in Java before an I/O operation is a classic **Time-of-Check to Time-of-Use (TOCTOU)** race condition. An in-memory flag check cannot protect against execution pauses that occur between the check and the network flush. Furthermore, the relational database lock table and the Kafka broker log have no distributed consensus bridge (no XA 2PC).

### Required Invariants
1. **Persistent UUIDv7 CloudEvents Metadata:**
   * Every outgoing Kafka record **MUST** include the CloudEvents v1.0 binary header `ce_id`.
   * `ce_id` **MUST** be mapped directly and immutably from the persisted database identifier (`outbox_event.id`, formatted as a time-ordered UUIDv7).
   * **AGENT FORBIDDEN PATTERN:** Never generate dynamic identifiers inside the dispatch loop (e.g., `UUID.randomUUID()` in memory). Doing so generates new message IDs on redelivery, blinding downstream consumers to replays.
2. **Consumer Deduplication Protocol:**
   * The library contract mandates **at-least-once delivery with consumer-side idempotency**. Downstream consumers evaluate `ce_id` against their deduplication filter to silently drop replays.
3. **Cooperative Fail-Fast Deadline Buffer:**
   * Worker threads **MUST** check the lease deadline before initiating each record dispatch:
     ```java
     if (leaseTimeRemaining.compareTo(Duration.ofSeconds(2)) < 0) {
         log.warnf("Lease safety deadline (< 2000ms) reached for bucket %d. Yielding batch to prevent split-brain race.", bucketId);
         break;
     }
     ```

---

## 2. Scenario 2: The Hash Reshuffling Catastrophe (Dynamic Scaling)

### Failure Mode
1. The outbox is configured with $M_1 = 16$ virtual buckets.
2. System throughput spikes; operators scale Kafka topic partitions to 64 and change `virtual-shards=64` in `application.properties`.
3. 15,000 un-dispatched events exist in `outbox_event`.
4. An event for aggregate `"order-99"` previously hashed to **Bucket 3** ($105991823 \pmod{16} = 3$).
5. A new event for `"order-99"` is inserted under the new configuration and hashes to **Bucket 19** ($105991823 \pmod{64} = 19$).
6. **Race Condition:** Pod A leases Bucket 19; Pod B leases Bucket 3. Both run concurrently. Pod A dispatches the *newer* event before Pod B dispatches the *older* event, permanently violating FIFO ordering.

### Required Invariants: Online Generation/Epoch Gating
1. **Persistent Generation Column:**
   * Schema mandates a `generation` column on `outbox_event` and `outbox_dead_letter`:
     ```sql
     CREATE INDEX idx_outbox_gen_bucket_fifo ON outbox_event (generation ASC, bucket_id ASC, next_retry_at ASC, id ASC);
     ```
2. **Configuration Updates:**
   * Resizing virtual buckets requires incrementing `quarkus.outbox.generation`:
     ```properties
     quarkus.outbox.generation=2
     quarkus.outbox.virtual-shards=64
     ```
3. **Zero-Downtime Poller Prioritization Barrier:**
   * Workers execute:
     ```sql
     SELECT COALESCE(MIN(e.generation), :activeGeneration) FROM OutboxEvent e
     ```
   * While $\min(\text{generation}) < \text{activeGeneration}$, all workers are strictly barred from polling Generation 2 buckets.
   * Generation 2 records accumulate safely in the database without pausing business domain writes.
   * The moment Generation 1 drains to 0 rows, $\min(\text{generation})$ evaluates to 2, seamlessly activating the 64-bucket topology without out-of-order delivery.

---

## 3. Scenario 3: The 2-Phase Commit Illusion (At-Least-Once Delivery Contract)

### Failure Mode
1. Outbox worker produces event to Kafka and awaits ACK.
2. The Kafka broker successfully commits the record and returns an ACK to the producer.
3. Before the database delete mutation executes, the Pod suffers an unrecoverable failure (OOM kill, node crash, power cut, DB connection timeout).
4. The outbox record remains in the database table.
5. A surviving Pod claims the bucket and republishes the record to Kafka.

### Required Invariants
1. **Acknowledged At-Least-Once Semantics:**
   * Without distributed XA transactions spanning Kafka and RDBMS, an outbox system is **strictly at-least-once**.
   * Downstream consumer contracts must mandate idempotency filters using the persistent UUIDv7 `ce_id`.
2. **Kafka Producer Idempotence Configuration:**
   * Outgoing Kafka producer **MUST** enforce Kafka-native idempotence to eliminate broker-side transient retry duplicates:
     ```properties
     kafka.enable.idempotence=true
     kafka.acks=all
     kafka.max.in.flight.requests.per.connection=5
     ```

---

## 4. Scenario 4: Connection Pool Starvation & Cascading Failure

### Failure Mode
1. Multiple pods run polling loops across virtual buckets.
2. Kafka brokers encounter transient latency or network backpressure.
3. Polling threads enter long blocking waits on Kafka ACKs while holding active database transactions open.
4. Agroal database connection pools reach `max-size`.
5. Background heartbeat threads wake up to renew bucket leases, cannot acquire a connection from the saturated pool, and block.
6. Leases expire in `outbox_lock`.
7. Peer Pods detect expired leases and attempt to claim them, compounding pool exhaustion (death spiral).

### Required Invariants
1. **The Short-Transaction Sandwich (MANDATORY):**
   * **NEVER hold a database connection across external network I/O.**
   * Phase 1: Short Read TX (StatelessSession select batch $\to$ commit & return connection).
   * Phase 2: Network Dispatch (Zero DB connections borrowed; stream records to Kafka and collect ACKs in memory).
   * Phase 3: Short Write TX (Bulk delete acknowledged IDs in a single statement).
2. **Physical Connection Pool Bulkhead:**
   * Lock acquisition and heartbeat tasks **MUST** execute against the dedicated Agroal named DataSource (`quarkus.datasource."outbox-lock"`), physically isolated from application business pools.
3. **Fail-Fast Heartbeat Supervision:**
   * Heartbeat renewal interval: $T_{\text{renew}} = \frac{1}{3} \times T_{\text{lease}}$.
   * If lease renewal fails past $\frac{2}{3} \times T_{\text{lease}}$, the supervisor sets `leaseLost = true` and interrupts the worker thread.

---

## 5. Scenario 5: The Aggregate Quarantine Poison Paradox

### Failure Mode
1. Event $E_{10}$ for `Order-123` encounters repeated serialization/broker rejections and hits `maxRetries`.
2. If the system simply drops $E_{10}$ to DLQ and continues unfreezing the bucket, a subsequent event $E_{11}$ (`Order-123-Cancelled`) will be dispatched downstream.
3. Downstream consumers receive a cancellation for an order that was never created, corrupting domain state machines.

### The Boundary Principle
**An infrastructure library must never guess business domain semantics.** The library must isolate poison pills, preserve per-aggregate consistency, loudly notify operators, and provide administrative remediation primitives.

### Required Invariants
1. **Two-Tiered Failure Model:**
   * **Transient Failures:** Back off exponentially via `retry_count` and `next_retry_at`. Release bucket lock so other buckets continue.
   * **Terminal Failures (`retry_count >= maxRetries`):**
     1. Archive failed event into `outbox_dead_letter` with status `DEAD`.
     2. Delete failed event from `outbox_event`.
     3. **Quarantine Trailing Aggregate Events:** Freeze all pending records sharing the same `aggregate_id`:
        ```sql
        UPDATE outbox_event 
        SET next_retry_at = '9999-12-31 23:59:59Z' 
        WHERE aggregate_id = :failedAggregateId;
        ```
     4. The rest of the virtual bucket continues draining normally.
2. **Operator Admin REST API (`/q/outbox-kafka/admin`):**
   * `GET /q/outbox-kafka/admin/quarantined`: List all quarantined aggregate IDs.
   * `GET /q/outbox-kafka/admin/quarantined/{id}/events`: Inspect dead-letter and frozen trailing events.
   * `POST /q/outbox-kafka/admin/quarantined/{id}/replay`: Reset `next_retry_at = now()` to retry.
   * `POST /q/outbox-kafka/admin/quarantined/{id}/skip`: Mark dead event as `SKIPPED` in DLQ and unfreeze pending events.
   * `DELETE /q/outbox-kafka/admin/quarantined/{id}`: Purge all pending and dead-letter records for the aggregate.
   * `POST /q/outbox-kafka/admin/drain`: Gracefully drain pending events across all buckets.

---

## 6. Summary Matrix of Invariants for Coding Agents

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

## 7. Automated Verification & Test Harness Directives

Refer to [`integration-tests/TEST_GUIDE.md`](file:///home/developer/projects/quarkus-outbox-kafka/integration-tests/TEST_GUIDE.md) for full implementation details of the 4 canonical test suites:
1. `PhantomProducerIT`
2. `ShortTransactionSandwichIT`
3. `AggregateQuarantineIT`
4. `OutboxConcurrencyIT`

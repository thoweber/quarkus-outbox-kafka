# Quarkus Outbox Kafka Extension

High-performance, database-agnostic **Transactional Outbox** extension for Quarkus microservices streaming events to Apache Kafka on Java 21 LTS.

Eliminates the operational overhead and database replication privileges of Change Data Capture (CDC / Debezium pipelines) while avoiding the throughput bottlenecks and race conditions of naive single-worker polling solutions.

---

## Key Architectural Capabilities

- **Active-Active Multi-Pod Scaling:** Uses virtual bucketing ($M = 1024$ virtual shards) and workload-aware greedy bin-packing to allow concurrent, non-colliding outbox drainage across multiple Kubernetes pods.
- **Strict Per-Aggregate FIFO Order:** Outbox records with the same `aggregateId` are partitioned deterministically and dispatched to Kafka in strict chronological sequence.
- **The Short-Transaction Sandwich:** Zero database transactions or connections are held open across network calls to Kafka brokers:
  1. *Short Read*: Fetch batch using Hibernate `StatelessSession` and immediately close the transaction.
  2. *Network Produce*: Dispatch records over the network to Kafka and collect broker ACKs.
  3. *Short Write*: Delete or update acknowledged event IDs in a separate, isolated transaction.
- **Direct Kafka Producer:** Built directly on `quarkus-kafka-client` (`KafkaProducer<String, String>`) for dynamic topic routing, pipelined batching into Kafka's `RecordAccumulator`, and CNCF CloudEvents v1.0 binary mode metadata.
- **100% ANSI JPA Portability:** Bypasses vendor-specific SQL upserts using pre-seeded lock rows and portable Compare-And-Swap (CAS) JPQL queries. Compatible with PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, and H2.
- **GraalVM Native Image Ready:** Full build-time augmentation, reflection registration, and native image validation via Mandrel.

---

## Project Structure

```
quarkus-outbox-kafka/
├── pom.xml                               # Aggregator parent (io.github.thoweber:quarkus-outbox-kafka-parent)
├── deployment/                           # Build-time processor module
│   ├── pom.xml
│   └── src/main/java/io/github/thoweber/quarkus/outbox/kafka/deployment/
│       └── QuarkusOutboxKafkaProcessor.java  # Bytecode generation & bean registration
├── runtime/                              # Extension runtime module
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
    └── src/test/java/io/github/thoweber/quarkus/outbox/kafka/it/
        └── OutboxConcurrencyIT.java      # PostgreSQL + Kafka container validation
```

---

## Build & Verification

The project enforces Java 21 LTS and uses Quarkiverse code formatting conventions inherited from `quarkiverse-parent`.

### Format Code
```bash
./mvnw process-sources
```
*Always run before committing. CI strictly validates formatting via `git diff --exit-code`.*

### Compile & Fast Unit Tests
```bash
./mvnw clean test-compile
./mvnw test
```

### Full Verification (JVM + Dev Services Containers)
```bash
./mvnw clean verify
```

### Run Integration Tests Specifically
```bash
./mvnw verify -pl integration-tests
```

### GraalVM Native Image Build
```bash
./mvnw verify -pl integration-tests -Dnative
```

---

## Continuous Integration (GitHub Actions)

The workflow defined in `.github/workflows/ci.yaml` is split into two phases:

1. **Build & Test (JVM)**: Runs on every push and pull request targeting `main` and `master`. Executes code formatting checks (`process-sources`) and the full reactor build (`clean verify`).
2. **Native Integration Tests**:
   - **On pushes / merges to `main` / `master`**: Runs automatically after the JVM build completes.
   - **On pull requests**: Configured as a mandatory manual review stage using GitHub Environment `native-integration-tests`.
   - **Auto-Invalidation**: Each new commit pushed to a PR branch automatically cancels and resets previous approvals using branch concurrency rules (`cancel-in-progress: true`).

---

## Documentation Links

- [ARCHITECTURE.md](file:///home/developer/projects/quarkus-outbox-kafka/ARCHITECTURE.md): Comprehensive arc42 architectural specification and Architectural Decision Records (ADRs).
- [SPECIFICATION.md](file:///home/developer/projects/quarkus-outbox-kafka/SPECIFICATION.md): Detailed protocol specifications, entities, schemas, algorithms, and configuration properties.
- [AGENTS.md](file:///home/developer/projects/quarkus-outbox-kafka/AGENTS.md): Operational guardrails, testing commands, and invariants for AI agents and human contributors.

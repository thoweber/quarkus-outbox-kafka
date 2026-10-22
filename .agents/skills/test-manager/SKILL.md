---
name: test-manager
description: Governs the 3-tier Quarkus extension testing taxonomy, mandatory invariant suites, flakiness zero-tolerance policy, and native compilation verification.
version: 1.0.0
---

# Test Manager & Quality Strategy Skill (`test-manager`)

This skill governs test architecture, execution tiers, mandatory invariant suites, flakiness mitigation, and GraalVM native image verification across **quarkus-outbox-kafka**.

---

## 1. Core Testing Principles

* **Real-World Fidelity over Heavy Mocking:** Prefer testing against real dependencies inside ephemeral environments (Quarkus Dev Services for PostgreSQL + Redpanda/Kafka) over fragile mock-heavy integration layers.
* **Shift-Left Fast Feedback:** Unit and augmentation checks must run in $< 3\text{ seconds}$ without booting Docker. Full Dev Services invariant suites run in $< 2\text{ minutes}$.
* **Zero-Flakiness Rule:** A non-deterministic test is a failing test. Tests that intermittently fail without code changes are quarantined immediately and marked as blocking until resolved.
* **Hermetic Execution:** Every test suite must seed its own data, use isolated bucket ranges or clean database states, and tear down without side effects.

---

## 2. The 3-Tier Extension Testing Pyramid

```text
                   / \
                  /   \       Tier 3: GraalVM Native Image Verification
                 /     \      (Mandrel 21 native binary, RSS bounds <= 96MB)
                /-------\
               /         \    Tier 2: Dev Services Invariant Suites
              /           \   (Real Postgres + Kafka Dev Services, 4 failure modes)
             /-------------\
            /               \ Tier 1: Deployment Augmentation Unit Tests
           /_________________\ (QuarkusUnitTest, build steps, Jandex, < 3s, zero Docker)
```

### Tier 1: Deployment Augmentation Unit Tests (`deployment/src/test`)
* **Scope:** `@BuildStep` methods in `QuarkusOutboxKafkaProcessor`, synthetic bean registration, JPA entity model indexing (`AdditionalJpaModelBuildItem`), unremovable bean definitions, and build-time configuration parsing.
* **Execution:** Fast in-memory runs via `QuarkusUnitTest` with Dev Services explicitly disabled (`quarkus.kafka.devservices.enabled=false`).
* **SLA:** Execution time $< 3\text{ seconds}$ for the entire deployment test suite.
* **Command:**
  ```bash
  ./mvnw test -pl deployment
  ```

### Tier 2: Dev Services Invariant Suites (`integration-tests/src/test`)
* **Scope:** Real multi-component lifecycle, database transactions, Kafka network sending, virtual thread concurrency, and distributed failure modes.
* **Execution:** `@QuarkusTest` / `@QuarkusIntegrationTest` executing against real PostgreSQL and Kafka Testcontainers.
* **Mandatory Suites:**
  1. `PhantomProducerIT`: Simulates STW GC pause, verifies lease steal by peer pod, late flush deduplication via persistent UUIDv7 `ce_id`.
  2. `ShortTransactionSandwichIT`: Asserts Agroal database connection pool reports exactly 0 active connections during Phase 2 Kafka network transit.
  3. `AggregateQuarantineIT`: Asserts poison pill reaches DLQ, trailing events for that aggregate freeze (`next_retry_at = 9999-12-31`), sibling aggregates in the same bucket drain cleanly, and admin `/replay` and `/skip` endpoints function properly.
  4. `OutboxConcurrencyIT`: Validates multi-pod concurrent bucket acquisition and asserts 100% monotonic FIFO arrival per aggregate with zero ordering anomalies.
* **Command:**
  ```bash
  ./mvnw verify -pl integration-tests
  ```

### Tier 3: GraalVM Native Image Verification (`integration-tests`)
* **Scope:** Ahead-of-Time (AOT) compilation validation with Mandrel 21 / GraalVM. Verifies reflection registrations for JPA entities and CloudEvents serializers, runtime initialization flags, and container RSS memory bounds.
* **SLA:** Idle RSS $\le 64\text{ MB}$, peak throughput RSS $\le 96\text{ MB}$.
* **Command:**
  ```bash
  ./mvnw verify -pl integration-tests -Dnative
  ```

---

## 3. Test Manager Directives & Quality Gates

When reviewing code, pull requests, or test execution plans:

1. **Reject Fragile In-Memory Mocks for Invariants:** Block attempts to test distributed locking or the transaction sandwich using mock database connections or mock Kafka producers. Invariants must be validated against real Dev Services containers.
2. **Protect TDD Feedback Speed:** Never let deployment unit tests depend on container startup. Keep Tier 1 strictly hermetic and Docker-free.
3. **Enforce AssertJ Exclusively:** Reject tests using JUnit `assertEquals` or `assertTrue`. All assertions must use AssertJ (`assertThat`, `assertThatThrownBy`).
4. **Enforce Monotonic FIFO Assertions:** Concurrency tests must verify strict per-aggregate monotonicity:
   ```java
   assertThat(receivedOffsetsPerAggregate).isStrictlyIncreasing();
   ```
5. **Enforce Flakiness Triage:** Any test using arbitrary `Thread.sleep()` must be refactored to use `org.awaitility.Awaitility`.

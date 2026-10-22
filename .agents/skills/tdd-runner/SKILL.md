---
name: tdd-runner
description: Autonomous Test-Driven Development (TDD) execution skill enforcing the Red-Green-Refactor loop, AssertJ rules, and distinguishing fast build-time unit tests from containerized ITs.
version: 1.0.0
---

# Test-Driven Development (TDD) Runner Skill (`tdd-runner`)

This skill governs the autonomous Test-Driven Development (TDD) execution flow tailored specifically for the **quarkus-outbox-kafka** Quarkus extension.

---

## 1. The Extension TDD Execution Loop

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                             1. RED PHASE                                    │
│  - Select correct test module based on scope:                               │
│    • Augmentation / Build-Time: deployment/src/test/ (*Test.java)           │
│    • Invariants / Failure Modes: integration-tests/src/test/ (*IT.java)     │
│  - Write failing test with Given-When-Then structure                        │
│  - Assert behavior strictly using AssertJ (assertThat, assertThatThrownBy)  │
│  - Run target test -> Confirm FAIL with expected assertion failure          │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            2. GREEN PHASE                                   │
│  - Implement minimal production code to satisfy failing assertions          │
│  - Adhere to architectural guardrails (StatelessSession, low allocation)    │
│  - Run target test -> Confirm PASS                                          │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           3. REFACTOR PHASE                                 │
│  - Format code: `./mvnw process-sources && git diff --exit-code`            │
│  - Verify module boundaries (no deployment imports in runtime)              │
│  - Run fast compile & unit verification: `./mvnw test-compile`              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Test Module Selection & Feedback Velocity

To preserve developer velocity, never boot heavy Docker containers for tests that can be verified at build time:

### A. Fast Build-Time Unit Tests (`deployment/src/test`)
* **Scope:** `@BuildStep` augmentation, bean indexing, bytecode generation, configuration property validation.
* **Harness:** `QuarkusUnitTest` with Docker disabled (`quarkus.kafka.devservices.enabled=false`).
* **SLA:** Execution time $< 3\text{ seconds}$.
* **Execution Command:**
  ```bash
  ./mvnw test -pl deployment -Dtest=TargetProcessorTest
  ```

### B. Dev Services Integration Tests (`integration-tests/src/test`)
* **Scope:** Multi-pod concurrency, lease failover, poison pill quarantining, Kafka network dispatch, database transaction sandwich.
* **Harness:** `@QuarkusTest` / `@QuarkusIntegrationTest` running against ephemeral PostgreSQL and Kafka Dev Services containers.
* **SLA:** Execution time $< 60\text{ seconds}$ per suite.
* **Execution Command:**
  ```bash
  ./mvnw test -pl integration-tests -Dtest=TargetInvariantIT
  ```

---

## 3. AssertJ Standard & Flakiness Rules

1. **AssertJ Mandatory:** Always use AssertJ fluent assertions:
   ```java
   // Correct
   assertThat(batchRecord.bucketId()).isBetween(0, 1023);
   assertThatThrownBy(() -> dispatcher.dispatch(poisonEvent))
       .isInstanceOf(QuarantineException.class);
   
   // Forbidden
   assertEquals(10, count);
   assertTrue(condition);
   ```

2. **Zero `Thread.sleep()`:** Never use `Thread.sleep()` in test assertions. Use Awaitility with explicit timeouts and poll intervals:
   ```java
   await().atMost(Duration.ofSeconds(5))
       .pollInterval(Duration.ofMillis(50))
       .untilAsserted(() -> assertThat(receivedRecords).hasSize(expectedCount));
   ```

3. **Hermetic State Cleanup:** Tests must never depend on state from prior tests. Virtual bucket locks and outbox tables must be cleared or partitioned cleanly per test case.

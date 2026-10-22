# AGENTS.md - Quarkus Outbox Extension

## Project Context
High-performance, database-agnostic Transactional Outbox extension for Quarkus.
Uses virtual bucketing to allow safe active-active multi-pod Kafka dispatching while preserving strict FIFO ordering per aggregate ID.

- **Baseline**: Java 21 LTS
- **Build Tool**: Apache Maven (>= 3.9.x)
- **Parent**: Inherits from `io.quarkiverse:quarkiverse-parent`
- **Core Dependencies**: Quarkus 3.15+, Hibernate ORM, Apache Kafka Client

---

## Module Layout & Boundary Rules

1. `runtime/`
    - Contains CDI beans, JPA entities (`OutboxEvent`, `OutboxLock`), lock providers, and Kafka dispatchers.
    - **Rule**: Never import `io.quarkus.deployment.*` classes in runtime.

2. `deployment/`
    - Contains build-time processors and `@BuildStep` methods.
    - Responsible for registering beans (`AdditionalBeanBuildItem`), indexing JPA entities (`AdditionalJpaModelBuildItem`), and generating synthetic bytecode.
    - **Rule**: Do not put runtime business execution logic here.

3. `integration-tests/`
    - Integration tests executing against real Quarkus Dev Services (PostgreSQL container + Kafka container).
    - Validates multi-pod concurrency, lease failover, and native compilation.

---

## Critical Build & Verification Commands

Always run these commands before submitting changes or finishing a task:

- **Format Code**:
  ```bash
  ./mvnw process-sources
  ```
  Note: CI strictly enforces Quarkus code formatting and import ordering inherited from `quarkiverse-parent`. If `git diff --exit-code` fails after compile, formatting was missed.

- **Fast Compile & Unit Tests:**
  ```bash
  ./mvnw test-compile
  ```

- **Full Verification (includes Dev Services container tests):**
  ```bash
  ./mvnw clean verify
  ```
- **Run Single Integration Test:**
  ```bash
  ./mvnw verify -pl integration-tests -Dit.test=OutboxWorkerIT
  ```
- **GraalVM Native Build Validation:**
   ```bash
   ./mvnw verify -pl integration-tests -Dnative
   ```

---

## Architectural Guardrails & Invariants

When writing or modifying code in this repository, strictly adhere to these architectural rules:

### 1. Database Transactions (The Short-Transaction Sandwich)

* NEVER hold an open database connection or transaction across external network calls (such as Kafka broker dispatches).
* Always use the 3-phase lifecycle:
  1. Short Read: Fetch a batch of eligible records and immediately commit/close the transaction.
  2. Network Send: Dispatch records to Kafka and collect successful/failed message acknowledgments in memory.
  3. Short Write: Issue a single batch delete (or retry update) for acknowledged IDs in a separate transaction.

### 2. Transaction Boundaries & CDI Proxies
* Do not call `@Transactional` methods on `this` within the same class; self-invocation bypasses ArC client proxies.
* Use `QuarkusTransaction.requiringNew().call(...)` or `QuarkusTransaction.requiringNew().run(...)` for programmatic transaction control.
* Alternatively, delegate database operations to an injected separate repository bean.

### 3. Concurrency & Locking
* Virtual bucket locks must be acquired using atomic compare-and-swap (CAS) updates against `OutboxLock`.
* Background heartbeat threads renewing bucket leases must be isolated from worker threads to prevent lease expiration during broker lag.
* When lease renewal fails, the worker must fail fast and abort the active batch before lease expiry.

### 4. Database Portability
* Stick to standard ANSI JPQL and standard JPA mappings (`Instant`, `@Lob` or large text).
* Do not introduce PostgreSQL-specific or MySQL-specific native SQL queries in default beans. Vendor-specific lock providers must live behind the `OutboxLockProvider` SPI.
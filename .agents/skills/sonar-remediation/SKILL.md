---
name: sonar-remediation
description: Static analysis, resource leak prevention, Agroal/StatelessSession cleanup, and code hygiene for quarkus-outbox-kafka.
version: 1.0.0
---

# Sonar Remediation & Resource Hygiene Skill (`sonar-remediation`)

This skill governs static analysis checks, code smell remediation, resource leak prevention, and Quarkiverse coding standards across **quarkus-outbox-kafka**.

---

## 1. Quality Gate Standards

* **Bugs & Vulnerabilities:** $0$ allowed (Grade A reliability).
* **Security Hotspots:** $0$ open hotspots.
* **Resource Leaks:** Strictly $0$ unclosed database connections, `StatelessSession` instances, or unmanaged virtual threads.
* **Formatting Compliance:** `./mvnw process-sources && git diff --exit-code` must pass cleanly without diffs.

---

## 2. Common Remediation Patterns for Outbox Systems

### A. Resource & Session Leak Prevention
Hibernate `StatelessSession` instances and JDBC connections from the `outbox-lock` pool must always be scoped within programmatic transactions or `try-with-resources`:

```java
// Correct: Programmatic transaction management
return QuarkusTransaction.requiringNew().call(() -> {
    try (StatelessSession session = sessionFactory.openStatelessSession()) {
        return session.createQuery("SELECT ...", OutboxBatchRecord.class).list();
    }
});

// Forbidden: Unclosed session or open connection leaking outside transaction
StatelessSession session = sessionFactory.openStatelessSession();
var list = session.createQuery(...).list();
```

### B. Kafka Callback Exception Handling
Never swallow exceptions or log and ignore failures inside `org.apache.kafka.clients.producer.Callback`. Errors must be captured in thread-safe memory buffers for Phase 3 short-write processing:

```java
// Correct: Capture metadata or error safely in memory
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        batchErrors.add(new DispatchError(record.key(), recordId, exception));
    } else {
        ackedIds.add(recordId);
    }
});

// Forbidden: Swallowing or ignoring exceptions
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        log.error("Failed to send", exception); // Error swallowed, record not retried!
    }
});
```

### C. Nullability Guardrails with JSpecify
Use JSpecify `@NullMarked` and `@Nullable` annotations across public methods and SPI boundaries:

```java
@NullMarked
public interface OutboxDispatcher {
    void dispatch(OutboxBatchRecord record);
    Optional<Instant> getQuarantinedSince(String aggregateId);
}
```
* Never return raw `null` for collections or optional values; return `List.of()`, `Set.of()`, or `Optional<T>`.

### D. Formatting & Import Hygiene
CI strictly enforces Quarkiverse import ordering and code formatting inherited from `quarkiverse-parent`.
* **Zero Wildcard Imports:** `import java.util.*;` is forbidden.
* **Format Command:** Always run `./mvnw process-sources` before checking in changes.
* **Verification:** Run `git diff --exit-code` to ensure formatting produced zero uncommitted changes.

---

## 3. Verification Commands

```bash
# Apply Quarkus / Quarkiverse formatting
./mvnw process-sources

# Verify zero formatting discrepancies
git diff --exit-code

# Execute fast compiler check
./mvnw test-compile
```

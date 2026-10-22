---
name: sla-benchmark-runner
description: Governs performance benchmarking, JMH allocation microbenchmarks, and Dev Services load testing against SLAs defined in config.json.
version: 1.0.0
---

# SLA Benchmark Runner Skill (`sla-benchmark-runner`)

This skill governs performance benchmarking, allocation profiling, and end-to-end load validation to verify compliance with the numerical Quality Goals defined in `.agents/config.json`.

---

## 1. SLA Thresholds & Targets

Read and enforce numerical targets from `.agents/config.json` (`qualityGoalsAndSLA`):

| Metric | Threshold | Configuration Key |
| :--- | :--- | :--- |
| **Drain Throughput** | $\ge 10{,}000\text{ msgs/sec}$ per pod | `drainThroughputMsgsPerSecMin` |
| **Transit Latency (p50)** | $\le 15\text{ ms}$ | `transitLatencyMsP50Max` |
| **Transit Latency (p95)** | $\le 50\text{ ms}$ | `transitLatencyMsP95Max` |
| **Transit Latency (p99)** | $\le 100\text{ ms}$ | `transitLatencyMsP99Max` |
| **DB Connection Hold Time (Phase 2)** | Strictly $0\text{ ms}$ | `databaseHoldTimeMsPhase2` |
| **Backlog Scan Latency (p99)** | $\le 2.0\text{ ms}$ | `backlogScanMsP99Max` |
| **Heap Allocation per Record** | $\le 512\text{ bytes}$ | `heapAllocationBytesPerRecordMax` |
| **JPA Entity Hydrations in Loop** | Exactly $0$ | `managedEntityHydrationsInLoop` |
| **GC Pause (ZGC / Generational ZGC)** | $\text{p99.9} \le 10\text{ ms}$ | `gcPauseZgcMsP999Max` |
| **GC Pause (G1 STW)** | Max $\le 50\text{ ms}$ | `gcPauseG1MaxStwMs` |
| **JVM Heap Overhead** | $\le 32\text{ MB}$ steady-state | `jvmHeapOverheadMbMax` |
| **Native Container RSS (Idle)** | $\le 64\text{ MB}$ | `nativeContainerRssIdleMbMax` |
| **Native Container RSS (Peak)** | $\le 96\text{ MB}$ | `nativeContainerRssPeakMbMax` |
| **Virtual Thread Stack Overhead** | $\le 1\text{ KB}$ shallow retained | `virtualThreadStackKbMax` |
| **In-Flight Memory Footprint** | $\le 5\text{ MB}$ ($B \times R$) | `inFlightMemoryMbMax` |
| **FIFO Order Anomalies** | Exactly $0$ | `fifoOrderAnomaliesMax` |
| **Duplicate Identification Rate** | $100\%$ (via UUIDv7 `ce_id`) | `duplicateIdentificationPercent` |
| **Heartbeat Success Rate** | $100\%$ under pool exhaustion | `heartbeatSuccessRatePercent` |

---

## 2. Microbenchmarks: JMH Allocation Profiling

JMH microbenchmarks measure per-operation heap allocations on the hot dispatch path.

### Execution Command
```bash
./mvnw clean test-compile -pl runtime -Dbenchmark=OutboxDispatchBenchmark
java -jar runtime/target/benchmarks.jar -prof gc -f 1 -wi 3 -i 5
```

### JMH Benchmark Template (`OutboxDispatchBenchmark.java`)
```java
package io.github.thoweber.quarkus.outbox.kafka.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class OutboxDispatchBenchmark {

    private String aggregateId;
    private byte[] payload;
    private UUID eventId;

    @Setup
    public void setup() {
        this.aggregateId = "order-123456789";
        this.payload = "{\"status\":\"COMPLETED\"}".getBytes(StandardCharsets.UTF_8);
        this.eventId = UUID.randomUUID();
    }

    @Benchmark
    public void benchmarkHeaderAndRecordAssembly(Blackhole bh) {
        // Enforces <= 512 bytes allocation per record
        var record = OutboxRecordAssembler.assemble(eventId, aggregateId, payload);
        bh.consume(record);
    }
}
```

### Quality Gate Assertion
If the allocation profiler reports $> 512\text{ bytes/op}$ (`alloc/op`), the benchmark fails. Check for:
* Uncached CloudEvents header keys (`byte[]` copies).
* Dynamic `String` formatting (`String.format()`).
* Intermediate stream/lambda object allocations.

---

## 3. Macrobenchmarks: Dev Services Throughput & Latency Suite

Located in `integration-tests/`, this test seeds 50,000 outbox records and measures end-to-end drain throughput and connection pool behavior.

### Execution Command
```bash
./mvnw test -pl integration-tests -Dtest=OutboxThroughputBenchmarkIT
```

### Assertions Enforced
1. **Drain Rate:** $\text{durationMs} \le \frac{\text{totalRecords}}{10.0}$ (verifying $\ge 10{,}000\text{ msgs/sec}$).
2. **Phase 2 Connection Hold Time:** Verify Agroal pool metrics during Kafka network dispatch show `activeConnections == 0`.
3. **Monotonic Sequence:** Verify that for every aggregate ID, received Kafka offsets have strictly increasing sequence numbers without gaps or inversions.

---

## 4. Memory & Footprint Verification (GraalVM Native)

To verify RSS thresholds:
```bash
# Build native binary
./mvnw clean verify -pl integration-tests -Dnative

# Run container and measure RSS
docker run --rm -m 128m -e QUARKUS_LOG_LEVEL=INFO outbox-kafka-it &
PID=$!
ps -o rss= -p $PID
```
Assert:
- Idle RSS $\le 64\text{ MB}$.
- Peak throughput RSS $\le 96\text{ MB}$.

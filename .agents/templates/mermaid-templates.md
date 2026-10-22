# Mermaid Diagram Templates for Quarkus Outbox Kafka

## 1. Short-Transaction Sandwich (3-Phase Dispatch Lifecycle)

```mermaid
sequenceDiagram
    participant Worker as Outbox Worker (Virtual Thread)
    participant DB as Database (StatelessSession)
    participant Kafka as Kafka Broker (RecordAccumulator)
    participant DLQ as Dead Letter Queue (StatelessSession)

    Note over Worker,DB: Phase 1: Short Read (Transaction 1)
    Worker->>DB: Open StatelessSession & Transaction
    Worker->>DB: Fetch batch of OutboxBatchRecord (Direct Projection)
    Worker->>DB: Commit & Close Transaction (DB Connection Released)
    Note over Worker: Active DB Connections = 0

    Note over Worker,Kafka: Phase 2: Network Transit (Zero DB Connection)
    loop For each record in batch
        Worker->>Worker: Check lease remaining duration >= 2s
        Worker->>Kafka: send(ProducerRecord) with aggregateId key & UUIDv7 ce_id
    end
    Kafka-->>Worker: Collect RecordMetadata acks & exceptions in memory

    Note over Worker,DB: Phase 3: Short Write (Transaction 2)
    alt All messages acknowledged successfully
        Worker->>DB: Open StatelessSession & Transaction
        Worker->>DB: Batch DELETE FROM OutboxEvent WHERE id IN (ackedIds)
        Worker->>DB: Commit & Close Transaction
    else Partial failure or poison pill detected
        Worker->>DB: Open StatelessSession & Transaction
        Worker->>DB: Batch DELETE successful IDs
        Worker->>DB: Quarantine failed aggregate (next_retry_at = 9999-12-31)
        Worker->>DLQ: INSERT INTO OutboxDeadLetter
        Worker->>DB: Commit & Close Transaction
    end
```

## 2. Multi-Pod Virtual Bucketing System Topology

```mermaid
graph TD
    subgraph Client Application
        App["Business Service / API"] -->|"1. RequiringNew Transaction"| DB_Write[("RDBMS (OutboxEvent)")]
    end

    subgraph Cluster Deployment (Active-Active Pods)
        subgraph Pod 1
            Worker1["Outbox Worker Pod 1"]
            Heartbeat1["Virtual Thread Heartbeat Supervisor"]
        end
        subgraph Pod 2
            Worker2["Outbox Worker Pod 2"]
            Heartbeat2["Virtual Thread Heartbeat Supervisor"]
        end
    end

    subgraph Database Infrastructure
        DB_Write --- LockTable[("OutboxLock (M = 1024 Rows)")]
        DB_Write --- EventTable[("OutboxEvent (Generation Partitioned)")]
    end

    subgraph Kafka Messaging Cluster
        Topic["Kafka Topic (Partitioned by Murmur2(aggregateId))"]
    end

    Heartbeat1 -->|"JPQL CAS Lease (Bulkhead outbox-lock Pool)"| LockTable
    Heartbeat2 -->|"JPQL CAS Lease (Bulkhead outbox-lock Pool)"| LockTable

    Worker1 -->|"Leases Buckets 0..511"| EventTable
    Worker2 -->|"Leases Buckets 512..1023"| EventTable

    Worker1 -->|"Network Send (ce_id UUIDv7)"| Topic
    Worker2 -->|"Network Send (ce_id UUIDv7)"| Topic
```

## 3. Aggregate Quarantining State Diagram

```mermaid
stateDiagram-v2
    [*] --> Active: Event Created (next_retry_at = now)
    
    Active --> Dispatched: Acked by Kafka Broker
    Dispatched --> [*]: Deleted from OutboxEvent
    
    Active --> RetryScheduled: Transient Kafka Failure (attempts < maxRetries)
    RetryScheduled --> Active: Backoff Window Elapsed
    
    Active --> Quarantined: Poison Pill (attempts >= maxRetries)
    
    state Quarantined {
        [*] --> Frozen: Copy to OutboxDeadLetter & next_retry_at = 9999-12-31
        Frozen --> TrailingFrozen: Subsequent events for aggregateId frozen
    }
    
    Quarantined --> Active: Admin POST /replay (Reset next_retry_at = now)
    Quarantined --> [*]: Admin POST /skip (Mark SKIPPED & Unfreeze trailing)
    Quarantined --> [*]: Admin DELETE (Purge aggregate events)
```

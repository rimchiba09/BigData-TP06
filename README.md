# Big Data – TP06: Stream Processing

## Task 6: Flink SQL – Event-Time Window Analysis

This task demonstrates how to process student event streams using **Apache Kafka** and **Apache Flink SQL**.

### Objective

Read student events from Kafka and calculate the number of `check-in` events for each department using one-minute event-time tumbling windows.

### Technologies

- Apache Kafka 4.1.1
- Apache Flink 1.20.5
- Kafka SQL Connector 3.4.0-1.20
- Python
- Flink SQL

### Kafka topic

```text
campus-events
```

### Flink SQL table

The `campus_events` table reads JSON events from Kafka. The `eventTime` field is used for event-time processing, with a watermark of 30 seconds.

```sql
CREATE TABLE campus_events (
    eventId INT,
    studentId STRING,
    eventType STRING,
    department STRING,
    eventTime TIMESTAMP_LTZ(3),
    WATERMARK FOR eventTime AS eventTime - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'tp06-flink',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
```

### Event-time window query

The following query counts check-in events per department in one-minute, non-overlapping windows.

```sql
SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkins
FROM TABLE(
    TUMBLE(
        TABLE campus_events,
        DESCRIPTOR(eventTime),
        INTERVAL '1' MINUTE
    )
)
WHERE eventType = 'check-in'
GROUP BY window_start, window_end, department;
```

### Repository contents

- `answers.tex` – LaTeX document containing only the answers to the Task 6 questions.
- `README.md` – Overview of the task, technologies, table definition, and window query.

### Result

The Flink SQL streaming query ran successfully and produced the number of student check-ins grouped by department and one-minute event-time windows.

---

**Course:** Big Data  
**Practical Work:** TP06 – Stream Processing  
**Task:** 6 – Flink SQL: Event-Time Window Analysis

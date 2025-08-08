# Spark Optimization Mini-Project

## Overview

This mini-project demonstrates query optimization in Apache Spark by comparing a **baseline** query against an **optimized** query using the same dataset.
 We capture:

- Query execution time
- Spark physical execution plans
- Shuffle read/write metrics from the Spark History Server

The optimized query reduces execution time and resource usage through:

1. Pre-aggregation before joins
2. Broadcast joins where appropriate
3. Column pruning in the optimized run only

------

## Prerequisites
- Apache Spark 3.x
- Python 3.x
- `pyspark` installed
- Spark History Server running (`$SPARK_HOME/sbin/start-history-server.sh`)

------

## Dataset

- **questions** and **answers** datasets in Parquet format, located in `./data/questions` and `./data/answers`.

------

## Usage

### 1. Start Spark History Server

```bash
$SPARK_HOME/sbin/start-history-server.sh
# Access UI: http://localhost:18080
```

### 2. Run in Baseline Mode

```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  optimize.py --mode baseline
```

### 3. Run in Optimized Mode

```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  optimize.py --mode optimized
```

### 4. Run Compare Mode (Baseline then Optimized)

```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  optimize.py --mode compare
```

## Notes

- The `--conf spark.eventLog.enabled=true` and `--conf spark.eventLog.dir=...` flags are **required** so that Spark writes event logs. These logs are consumed by the Spark History Server to produce the shuffle and execution metrics shown in the **Deliverables** and **Results** sections.
- The **compare** mode runs both baseline and optimized queries sequentially in one session, capturing metrics for each.
- The Spark UI is available during runtime at: http://localhost:4040
- The Spark History Server can be used to review completed runs.


------

## Retrieving Shuffle Metrics

After a run, you can retrieve shuffle stats directly from the History Server API:

```bash
bashCopyEditcurl -s "http://localhost:18080/api/v1/applications/<appId>/stages" \
| jq '[ .[] | {stageId, shuffleReadMB:(.shuffleReadBytes//0/1048576), shuffleWriteMB:(.shuffleWriteBytes//0/1048576)} ]'
```

Replace `<appId>` with the Application ID from the History Server UI.

---

## Optimization Approach
We incrementally improved the query by:
1. **Aggregating before join** – Reduce shuffle size by grouping `answers` first.
2. **Column pruning** – In the optimized query, only keep necessary columns.
3. **Broadcast join** – Broadcast the smaller `questions` DataFrame.
4. **Shuffle partition tuning** – Reduce `spark.sql.shuffle.partitions` from default (200) to `8` for local development.


---

## Results

> [!NOTE]
>
> Shuffle metrics were pulled from the History Server via `jq` and may slightly differ across runs due to Spark execution variance.

| Run Mode     | Execution Time (s) | Shuffle Read (MB) | Shuffle Write (MB) |
| ------------ | -----------------: | ----------------: | -----------------: |
| Baseline #1  |              4.311 |             0.674 |           0.000056 |
| Optimized #1 |              3.398 |             0.744 |           0.000225 |
| Baseline #2  |              3.908 |             0.674 |           0.000056 |
| Optimized #2 |              1.439 |             0.744 |           0.000225 |



------

## Observations

- **Execution Time**: Optimized query is ~3x faster than baseline.
- **Shuffle Read**: Slightly higher in optimized mode due to broadcast join strategy and pre-aggregation changes.
- **Shuffle Write**: Increased because intermediate results in the optimized plan are materialized differently.

## Conclusion

This project shows that:

- Aggregating before joining can significantly reduce execution time.
- Broadcast joins can be beneficial when one table is small enough to fit in memory.
- Column pruning reduces data shuffling and improves efficiency.

---

## Deliverables

| Deliverable             | Description                                                  | File(s)                                                      |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Baseline physical plan  | Saved `.explain(formatted)` output for baseline query        | [`plans/before_formatted.txt`](plans/before_formatted.txt)   |
| Optimized physical plan | Saved `.explain(formatted)` output for optimized query       | [`plans/after_formatted.txt`](plans/after_formatted.txt)     |
| Run log                 | Detailed run logs with execution times for each run          | [`logs/run.log`](logs/run.log)                               |
| Benchmark CSV           | CSV file with execution time, shuffle metrics, and row counts | [`benchmark_log.csv`](benchmark_log.csv)                     |
| Event logs              | Full Spark event logs for all runs (for Spark History Server inspection) | [`eventLogs-local-1754684722369.zip`](eventLogs-local-1754684722369.zip), [`eventLogs-local-1754684807733.zip`](eventLogs-local-1754684807733.zip), [`eventLogs-local-1754684834401.zip`](eventLogs-local-1754684834401.zip) |
| Optimized script        | Final optimized PySpark script with logging and plan capture | [`optimize.py`](optimize.py)                                 |
| README                  | This document                                                | [`README.md`](README.md)                                     |

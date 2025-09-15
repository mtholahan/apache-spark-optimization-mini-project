# Apache Spark Optimization Mini Project


## 📖 Abstract
This project reimplements an automobile post-sales reporting system using Apache Spark, showcasing Spark’s efficiency compared to legacy Hadoop MapReduce approaches. The dataset contains historical vehicle incidents, including initial sales, private resales, repairs, and accidents. The business goal is to generate a consolidated report of total accident counts per vehicle make and year, a valuable metric for second-hand buyers assessing vehicle reliability.

The workflow leverages Spark RDD transformations and actions to propagate vehicle metadata (make, year) from initial sale records into associated accident records, keyed by VIN. With Spark’s groupByKey, each VIN’s records are grouped, enriched, and transformed into accident-only outputs carrying complete context. The pipeline then maps make–year pairs, applies reduceByKey aggregations, and produces a count of accidents for each brand/year combination.

Outputs are persisted in HDFS as CSV, and the job is packaged with a submission shell script for execution via spark-submit. This redesign illustrates how Spark’s in-memory computation and concise functional APIs (map, flatMap, reduceByKey) streamline workflows that otherwise require verbose, multi-stage MapReduce jobs.

Through this project, I gained hands-on skills in RDD-based ETL, grouping and aggregation, HDFS integration, and Spark job deployment, while appreciating Spark’s superiority in developer productivity and performance.



## 🛠 Requirements
- Apache Spark 3.x (local mode or cluster)
- PySpark installed
- Provided dataset + optimize.py script from project archive
- GitHub repo with:
  - Original unoptimized code
  - Optimized code
  - README explaining improvements
  - Execution logs + query plans



## 🧰 Setup
- Download and extract project archive
- Verify optimize.py is present
- Place dataset in project folder
- Ensure Spark is running locally
- Run initial job with: spark-submit optimize.py



## 📊 Dataset
- Input dataset packaged in project archive
- Used for Q&A query aggregation task
- Schema: questions, answers, timestamps (monthly grouping)



## ⏱️ Run Steps
- Run baseline code: spark-submit optimize.py (unoptimized version)
- Capture EXPLAIN plan (saved in evidence/before_formatted.txt)
- Rewrite query with:
  - Appropriate Spark operators
  - Fewer shuffles
  - Better partitioning and caching
- Rerun optimized code
- Capture new EXPLAIN plan (evidence/after_formatted.txt)
- Save runtime log to logs/run.log



## 📈 Outputs
- Execution logs (logs/run.log)
- Query plans:
  - evidence/before_formatted.txt (unoptimized)
  - evidence/after_formatted.txt (optimized)
- Optimized PySpark script showing performance improvements



## 📸 Evidence

![run_log.png](./evidence/run_log.png)  
Screenshot excerpt from logs/run.log showing optimized run completion

![before_plan.png](./evidence/before_plan.png)  
Screenshot of unoptimized EXPLAIN plan (before_formatted.txt)

![after_plan.png](./evidence/after_plan.png)  
Screenshot of optimized EXPLAIN plan (after_formatted.txt)




## 📎 Deliverables

- [`- optimize.py (with improvements)`](./deliverables/- optimize.py (with improvements))

- [`- README.md describing issues and optimizations applied`](./deliverables/- README.md describing issues and optimizations applied)

- [`- Raw execution log: deliverables/log_run.txt`](./deliverables/- Raw execution log: deliverables/log_run.txt)

- [`- Query plans: deliverables/plan_before.txt and deliverables/plan_after.txt`](./deliverables/- Query plans: deliverables/plan_before.txt and deliverables/plan_after.txt)

- [`- Evidence screenshots in /evidence/`](./deliverables/- Evidence screenshots in /evidence/)




## 🛠️ Architecture
- Single-node Spark environment
- Job pipeline:
  - Load Q&A dataset
  - Aggregate answers by question and month
  - Compare unoptimized vs optimized transformations
- Improvements applied:
  - Operator selection
  - Reduced shuffles
  - Partitioning tuning



## 🔍 Monitoring
- Compared runtime metrics in logs/run.log
- Inspected EXPLAIN query plans before and after optimization
- Verified reduced shuffle stages and improved execution DAG



## ♻️ Cleanup
- Remove intermediate logs/ and evidence/ directories if not needed
- Stop local Spark session



*Generated automatically via Python + Jinja2 + SQL Server table `tblMiniProjectProgress` on 09-15-2025 00:57:54*
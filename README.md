# Apache Spark Optimization Mini Project


## 📖 Abstract
The mini project centers around optimizing an existing PySpark script (original_optimize.py). The script performs a query that retrieves the number of answers per question per month. The original implementation is suboptimal and must be improved using Spark performance techniques. The goal is to learn how to interpret Spark physical plans and apply tuning strategies that reduce job latency and improve scalability.

The original query joined the full questions and answers datasets, then grouped the result — causing unnecessary shuffling and memory usage. The optimized version improves this in several key ways:

- Aggregated first, joined later — reduces data volume early, shrinking the amount of data shuffled across the cluster.
- Column pruning — selected only required fields before any transformations to reduce memory and I/O.
- Explicit shuffle tuning — set spark.sql.shuffle.partitions = 8 to reduce the number of shuffle tasks.
- Improved date grouping — used date_trunc() instead of month() to support accurate year-month time-based aggregation.
- Attempted broadcast optimization — included a broadcast() hint to minimize join overhead, though Spark's join type limited its effect in this context.

These changes reduced the runtime from 9.62s to 6.01s and increased result accuracy by preventing cross-year month collisions.



## 🛠 Requirements
I ran this project in WSL/Ubuntu environment with PySpark installed.
- Apache Spark (local installation)
- PySpark (Python Spark bindings)
- original_optimize.py starter script
- Local folder structure for inputs and outputs
- Understanding of joins, aggregations, and query optimization in Spark



## 🧰 Setup
- Download the .zip archive from the project link
- Extract contents locally
- Launch PySpark session
- Open original_optimize.py and run the baseline query
- Modify query logic to apply optimizations



## 📊 Dataset
- Dataset is bundled with the archive
- Contains questions and answers table (exact schema not specified)
- Used to compute monthly answer counts per question



## ⏱️ Run Steps
- Run original original_optimize.py script to observe baseline performance
- Review Spark physical plan (e.g., via explain() or Spark UI)
- Apply optimizations:
- Reduce shuffle
- Re-partition strategically
- Use efficient operators
- Re-run optimized version and compare results



## 📈 Outputs
- Improved version of original_optimize.py
- Performance metrics before and after optimization
- Optional screenshots of Spark UI DAG or physical plan
- Written summary of what changed and why



## 📸 Evidence

![01_original_script_output.png](./evidence/01_original_script_output.png)  
Screenshot of PySpark output of original Python script

![02_revised_script_output.png](./evidence/02_revised_script_output.png)  
Screenshot of PySpark output of revised Python script




## 📎 Deliverables

- [`original_optimize.py`](./deliverables/original_optimize.py)

- [`revised_optimize.py`](./deliverables/revised_optimize.py)




## 🛠️ Architecture
- Local Spark environment using PySpark CLI
- Input data loaded from local files
- Single-node execution context
- No database or external services involved
- Job structure: Input CSV -> Transformations -> Aggregation -> Output (if any)



## 🔍 Monitoring
- Manual tracking of execution time before and after optimization
- explain() output or Spark UI DAG for logical plan comparison
- Memory use and shuffle metrics reviewed via logs or UI



## ♻️ Cleanup
- Delete temporary files from local directory
- Archive both versions of the script
- Optional: Document optimization strategies in project repo


*Generated automatically via Python + Jinja2 + SQL Server table `tblMiniProjectProgress` on 09-16-2025 17:35:44*
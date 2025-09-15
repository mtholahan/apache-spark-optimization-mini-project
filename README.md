# Apache Spark Optimization Mini Project


## 📖 Abstract
The mini project centers around optimizing an existing PySpark script (optimize.py). The script performs a query that retrieves the number of answers per question per month. The original implementation is suboptimal and must be improved using Spark performance techniques. The goal is to learn how to interpret Spark physical plans and apply tuning strategies that reduce job latency and improve scalability.



## 🛠 Requirements
- Apache Spark (local installation)
- PySpark (Python Spark bindings)
- optimize.py starter script
- Local folder structure for inputs and outputs
- Understanding of joins, aggregations, and query optimization in Spark



## 🧰 Setup
- Download the .zip archive from the project link
- Extract contents locally
- Launch PySpark session
- Open optimize.py and run the baseline query
- Modify query logic to apply optimizations



## 📊 Dataset
- Dataset is bundled with the archive
- Contains questions and answers table (exact schema not specified)
- Used to compute monthly answer counts per question



## ⏱️ Run Steps
- Run original optimize.py script to observe baseline performance
- Review Spark physical plan (e.g., via explain() or Spark UI)
- Apply optimizations:
- Reduce shuffle
- Re-partition strategically
- Use efficient operators
- Re-run optimized version and compare results



## 📈 Outputs
- Improved version of optimize.py
- Performance metrics before and after optimization
- Optional screenshots of Spark UI DAG or physical plan
- Written summary of what changed and why



## 📸 Evidence

![original_dag.png](./evidence/original_dag.png)  
Screenshot of Spark physical plan before optimization

![optimized_dag.png](./evidence/optimized_dag.png)  
Screenshot of Spark physical plan after optimization

![performance_diff_table.png](./evidence/performance_diff_table.png)  
Screenshot or table showing execution time and shuffle reduction

![code_diff_snippet.png](./evidence/code_diff_snippet.png)  
Screenshot of code comparing original and optimized logic




## 📎 Deliverables

- [`- Original and optimized versions of optimize.py`](./deliverables/- Original and optimized versions of optimize.py)

- [`- Performance summary notes stored in /deliverables/`](./deliverables/- Performance summary notes stored in /deliverables/)

- [`- Spark UI DAG screenshots showing before/after improvements`](./deliverables/- Spark UI DAG screenshots showing before/after improvements)

- [`- README explaining bottlenecks and applied optimizations`](./deliverables/- README explaining bottlenecks and applied optimizations)




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



*Generated automatically via Python + Jinja2 + SQL Server table `tblMiniProjectProgress` on 09-15-2025 19:26:53*
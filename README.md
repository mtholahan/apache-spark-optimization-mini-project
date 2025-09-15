# Apache Spark Optimization Mini Project


## 📖 Abstract
This project focuses on performance tuning within the Apache Spark framework using Databricks. A customer churn dataset is processed in a Spark job to perform analytics. The project explores how different optimization techniques such as caching strategies, partitioning, and joins affect the job's execution plan and performance. Students are asked to identify bottlenecks using the Spark UI and propose improvements to reduce job completion time. The goal is to understand the mechanics of distributed computing and gain hands-on experience in debugging and optimizing Spark applications.



## 🛠 Requirements
- Databricks Community Edition or equivalent Spark platform
- Apache Spark 3.x
- Customer churn dataset (CSV format)
- Familiarity with Spark DataFrames, Spark SQL
- Usage of Spark UI for job analysis
- Knowledge of performance tuning techniques (partitioning, caching, broadcast joins)



## 🧰 Setup
- Upload the provided churn dataset to Databricks
- Create a new notebook and import the starter code
- Run the initial job and observe Spark UI metrics
- Make iterative changes to optimize execution (e.g., change join type, cache intermediate data)
- Re-run and compare job performance in Spark UI



## 📊 Dataset
- Customer churn dataset (CSV format)

- Fields include: customer ID, gender, tenure, services used, contract type, payment method, churn label

 Used as input to Spark DataFrame transformations and aggregations



## ⏱️ Run Steps
- Load data into Spark DataFrame
- Conduct exploratory transformations and aggregations
- Use Spark UI to observe job stages and execution plans
- Apply optimizations (cache, repartition, broadcast joins)
- Track changes in execution metrics and document performance improvements



## 📈 Outputs
- Optimized Spark job with reduced runtime
- Documented comparison of execution plans and job metrics (before/after optimization)
- Discussion of trade-offs and rationale for tuning strategies
- Screenshots or logs from Spark UI showing reduced stages/tasks or memory usage



## 📸 Evidence

![execution_plan_before.png](./evidence/execution_plan_before.png)  
Screenshot of Spark UI execution plan before optimizations

![execution_plan_after.png](./evidence/execution_plan_after.png)  
Screenshot of Spark UI execution plan after optimizations

![spark_ui_metrics_table.png](./evidence/spark_ui_metrics_table.png)  
Screenshot of Spark UI metrics comparing job duration and shuffle activity

![tuning_code_snippet.png](./evidence/tuning_code_snippet.png)  
Screenshot of notebook section showing use of caching and broadcast joins




## 📎 Deliverables

- [`- - Optimized Spark notebook implementing performance improvements`](./deliverables/- - Optimized Spark notebook implementing performance improvements)

- [`- Python or Databricks notebook file stored in /deliverables/`](./deliverables/- Python or Databricks notebook file stored in /deliverables/)

- [`- Summary notes comparing performance before and after optimization`](./deliverables/- Summary notes comparing performance before and after optimization)

- [`- Spark UI execution plan screenshots saved in /evidence/`](./deliverables/- Spark UI execution plan screenshots saved in /evidence/)

- [`- README with overview of tuning techniques and setup instructions`](./deliverables/- README with overview of tuning techniques and setup instructions)




## 🛠️ Architecture
- Single-node or cluster Databricks environment
- Input CSV → Spark DataFrame
- Transformations/joins → Optimization via cache/broadcast
- Spark UI used for monitoring
- Output not persisted — focus is on tuning not data storage



## 🔍 Monitoring
- Spark UI used to monitor:
-   Job duration
-   Shuffle read/write
-   Memory usage
-   Number of stages/tasks
- Visual DAG inspection to validate optimization impact



## ♻️ Cleanup
- No explicit resource cleanup needed in Databricks

- Clear cached DataFrames (unpersist()) if needed

- Remove test datasets or notebooks after submission if storing externally



*Generated automatically via Python + Jinja2 + SQL Server table `tblMiniProjectProgress` on 09-15-2025 18:03:56*
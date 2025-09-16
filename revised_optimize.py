"""
Spark Optimization Mini-Project — Simplified Optimized Version
Applies Spark best practices:
- Column pruning
- Aggregation before join
- Broadcast join
- Fewer shuffles
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from io import StringIO
import sys

# Spark session
spark = SparkSession.builder \
    .appName("SparkOptimizationSimplified") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Set paths
project_path = os.path.dirname(os.path.abspath(__file__))
answers_path = os.path.join(project_path, "data", "answers")
questions_path = os.path.join(project_path, "data", "questions")

# ✅ Load only needed columns
answersDF = spark.read.parquet(answers_path).select("question_id", "creation_date")
questionsDF = spark.read.parquet(questions_path).select("question_id", "title")

# Optimized aggregation
start = time.time()

answers_month = (
    answersDF
    .withColumn("month", F.date_trunc("month", F.col("creation_date")))
    .groupBy("question_id", "month")
    .agg(F.count("*").alias("cnt"))
)

resultDF = (
    F.broadcast(answers_month)
    .join(questionsDF, "question_id", "right")
    .select("question_id", "creation_date", "title", "month", "cnt")
)

resultDF.orderBy("question_id", "month").show()

# Metrics
row_count = resultDF.count()
unique_questions = resultDF.select('question_id').distinct().count()


# Output
print(f"\n✅ Row Count: {row_count}")
print(f"✅ Unique Questions: {unique_questions}")
print(f"⏱️ Elapsed Time: {round(time.time() - start, 2)} seconds")


# 🔍 Save Spark Query Plans
# Save basic (non-formatted) plan
with open("after_optimization_query_plan.txt", "w") as f:
    f.write(resultDF._jdf.queryExecution().toString())

# Capture and save formatted plan
old_stdout = sys.stdout
sys.stdout = mystdout = StringIO()
resultDF.explain(mode='formatted')
sys.stdout = old_stdout

with open("after_optimization_query_plan.txt", "w") as f:
    f.write(mystdout.getvalue())

# Close the curtain
spark.stop()

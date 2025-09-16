'''
Optimize the query plan
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month
import os
import time
import sys
from io import StringIO


# Start Spark
spark = SparkSession.builder.appName('Optimize I').getOrCreate()

# Set paths
project_path = os.path.dirname(os.path.abspath(__file__))
answers_input_path = os.path.join(project_path, 'data', 'answers')
questions_input_path = os.path.join(project_path, 'data', 'questions')

# ✅ Load the DataFrames
answersDF = spark.read.option('path', answers_input_path).load()
questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we get number of answers per question per month
'''

start = time.time()

answers_month = answersDF.withColumn('month', month('creation_date')) \
                         .groupBy('question_id', 'month') \
                         .agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id', "left") \
                      .select('question_id', 'creation_date', 'title', 'month', 'cnt')

# Output result (to ensure lazy evaluation happens)
resultDF.orderBy('question_id', 'month').show()

# Metrics
row_count = resultDF.count()
unique_questions = resultDF.select('question_id').distinct().count()

# Output
print(f"\n✅ Row Count: {row_count}")
print(f"✅ Unique Questions: {unique_questions}")
print(f"⏱️  Total Time: {round(time.time() - start, 2)} seconds")

# 🔍 Save Spark Query Plans
# Save basic (non-formatted) plan
with open("before_optimization_query_plan.txt", "w") as f:
    f.write(resultDF._jdf.queryExecution().toString())

# Capture and save formatted plan
old_stdout = sys.stdout
sys.stdout = mystdout = StringIO()
resultDF.explain(mode='formatted')
sys.stdout = old_stdout

with open("before_optimization_query_plan.txt", "w") as f:
    f.write(mystdout.getvalue())

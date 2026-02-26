from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
# Create Spark session
spark = SparkSession.builder.appName("Aggregation Practice").getOrCreate()

# Sample dataset
data = [
    ("IT", 30000),
    ("IT", 40000),
    ("IT", 30000),
    ("HR", 25000),
    ("HR", 30000),
    ("FIN", 35000),
    ("FIN", 35000)
]

df = spark.createDataFrame(data, ["Dept", "Salary"])

print("Original Data")
df.show()


print("Global Aggregations")

df.agg(
    mean("Salary").alias("Mean Salary"),
    avg("Salary").alias("Average Salary"),
    sum("Salary").alias("Total Salary"),
    min("Salary").alias("Minimum Salary"),
    max("Salary").alias("Maximum Salary"),
    count("Salary").alias("Total Count"),
    countDistinct("Salary").alias("Unique Salaries")
).show()


print("Department-wise Aggregations")

df.groupBy("Dept").agg(
    mean("Salary").alias("Mean Salary"),
    avg("Salary").alias("Average Salary"),
    sum("Salary").alias("Total Salary"),
    min("Salary").alias("Minimum Salary"),
    max("Salary").alias("Maximum Salary"),
    count("Salary").alias("Count"),
    countDistinct("Salary").alias("Unique Salaries"),
    collect_list("Salary").alias("Salary List"),
    collect_set("Salary").alias("Unique Salary Set"),
    first("Salary").alias("First Salary"),
    last("Salary").alias("Last Salary")
).show(truncate=False)
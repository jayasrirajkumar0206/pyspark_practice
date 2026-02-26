from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("NumericFunctionsPractice").getOrCreate()

# Sample dataset
data = [
    ("Jaya", 22, 30000.456),
    ("Rahul", 25, -40000.789),
    ("Meena", 23, 35000.123),
    ("Arun", 26, 50000.987)
]

df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

print("Original Data")
df.show()

df.select(
    sum("Salary").alias("Total Salary"),
    avg("Salary").alias("Average Salary"),
    mean("Salary").alias("Mean Salary"),
    min("Salary").alias("Minimum Salary"),
    max("Salary").alias("Maximum Salary")
).show()

df.select(
    "Name",
    "Salary",

    abs("Salary").alias("Absolute Salary"),

    round("Salary", 2).alias("Rounded Salary")
).show()
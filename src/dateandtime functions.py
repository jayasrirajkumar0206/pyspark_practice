from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("DateTimePractice").getOrCreate()

# Sample data
data = [
    ("Jaya", "2024-01-10"),
    ("Rahul", "2023-12-05"),
    ("Meena", "2022-07-20")
]

df = spark.createDataFrame(data, ["Name", "JoinDate"])

print("Original Data")
df.show()

df = df.withColumn("JoinDate", to_date("JoinDate"))
df.printSchema()

df.select(
    "Name",
    "JoinDate",

    current_date().alias("Current Date"),
    current_timestamp().alias("Current Timestamp"),

    date_add("JoinDate", 10).alias("JoinDate + 10 Days"),

    datediff(current_date(), "JoinDate").alias("Days Difference"),

    year("JoinDate").alias("Year"),
    month("JoinDate").alias("Month"),
    day("JoinDate").alias("Day"),

    date_format("JoinDate", "dd-MM-yyyy").alias("Formatted Date")

).show(truncate=False)
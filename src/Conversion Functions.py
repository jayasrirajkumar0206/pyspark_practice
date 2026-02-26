from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("ConversionFunctionsPractice").getOrCreate()

# Sample dataset (all columns as string)
data = [
    ("Jaya", "22", "30000.50", "2024-01-10", "10-01-2024 10:30:00"),
    ("Rahul", "25", "40000.75", "2023-12-05", "05-12-2023 08:45:00"),
    ("Meena", "23", "35000.25", "2022-07-20", "20-07-2022 09:15:00")
]

df = spark.createDataFrame(
    data,
    ["Name", "Age", "Salary", "JoinDate", "JoinTimestamp"]
)

print("Original Data")
df.show()
df.printSchema()
#Using cast() for Numeric and Date Conversion
df2 = df.select(
    "Name",

    col("Age").cast("int").alias("Age"),
    col("Salary").cast("double").alias("Salary"),
    col("JoinDate").cast("date").alias("JoinDate")
)

print("Using CAST")
df2.show()
df2.printSchema()
#Using to_date() for Custom Format
df3 = df.select(
    "Name",
    to_date("JoinDate", "yyyy-MM-dd").alias("JoinDate")
)

df3.show()
#Using to_timestamp()
df4 = df.select(
    "Name",
    to_timestamp("JoinTimestamp", "dd-MM-yyyy HH:mm:ss").alias("JoinTimestamp")
)

df4.show()
df4.printSchema()
#Using selectExpr() (SQL Style)
df.selectExpr(
    "Name",
    "CAST(Age AS INT) AS Age",
    "CAST(Salary AS DOUBLE) AS Salary"
).show()
#Convert Multiple Columns Together
df_final = df.select(
    "Name",
    col("Age").cast("int"),
    col("Salary").cast("double"),
    to_date("JoinDate"),
    to_timestamp("JoinTimestamp", "dd-MM-yyyy HH:mm:ss")
)

df_final.show()
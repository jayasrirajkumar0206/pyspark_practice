from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("StringFunctionsPractice").getOrCreate()

# Sample dataset
data = [
    ("  jaya kumar  ", "jaya@gmail.com", "IT-2024"),
    ("rahul", "rahul@yahoo.com", "HR-2023"),
    ("  meena devi", "meena@gmail.com", "FIN-2022")
]

df = spark.createDataFrame(data, ["Name", "Email", "Code"])

print("Original Data")
df.show(truncate=False)

df.select(
    "Name",
    upper("Name").alias("Uppercase"),
    lower("Name").alias("Lowercase"),
    initcap("Name").alias("Initcap")
).show(truncate=False)


df.select(
    "Name",
    trim("Name").alias("Trim"),
    ltrim("Name").alias("Left Trim"),
    rtrim("Name").alias("Right Trim")
).show(truncate=False)


df.select(
    "Code",
    substring("Code", 1, 2).alias("Substring"),
    substring_index("Email", "@", 1).alias("Username")
).show(truncate=False)


df.select(
    "Email",
    split("Email", "@").alias("Split Email"),
    instr("Email", "@").alias("Position of @")
).show(truncate=False)


df.select(
    "Name",
    lpad("Name", 15, "*").alias("Left Pad"),
    rpad("Name", 15, "*").alias("Right Pad"),
    repeat("Name", 2).alias("Repeat")
).show(truncate=False)


df.select(
    "Email",
    regex_replace("Email", "gmail", "company").alias("Replaced"),
    regex_extract("Code", "(\\d+)", 1).alias("Extract Year")
).show(truncate=False)


df.select(
    "Name",
    length("Name").alias("Length")
).show()


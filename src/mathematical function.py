from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("MathFunctionsPractice").getOrCreate()

# Sample dataset
data = [
    ("A", 10.5),
    ("B", -20.3),
    ("C", 30.7),
    ("D", 5.2)
]

df = spark.createDataFrame(data, ["Name", "Value"])

print("Original Data")
df.show()

df.select(
    "Name",
    "Value",

    # Absolute value
    abs("Value").alias("ABS"),

    # Rounding
    ceil("Value").alias("CEIL"),
    floor("Value").alias("FLOOR"),

    # Exponential
    exp("Value").alias("EXP"),

    # Log (use abs to avoid NULL for negative)
    log(abs("Value")).alias("LOG"),

    # Power and root
    power("Value", 2).alias("SQUARE"),
    sqrt(abs("Value")).alias("SQRT")

).show()
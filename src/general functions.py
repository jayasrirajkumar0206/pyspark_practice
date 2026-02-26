from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("GeneralFunctionsPractice").getOrCreate()

# Sample dataset
data = [
    ("Jaya", 22, "IT", 30000),
    ("Rahul", 25, "HR", 40000),
    ("Meena", 23, "Finance", 35000),
    ("Arun", 26, "IT", 45000)
]

df = spark.createDataFrame(data, ["Name", "Age", "Dept", "Salary"])

print("Original Data")
df.show()


# Display first 20 rows
df.show()

# Display first 2 rows
df.show(2)

# Show without truncating text
df.show(truncate=False)


# collect(): returns all rows to Python
rows = df.collect()
print(rows)

# take(): returns first n rows
print(df.take(2))


# Structure of DataFrame
df.printSchema()

# Column names
print(df.columns)


# Select specific columns
df.select("Name", "Salary").show()

# Filter rows
df.filter(df.Salary > 35000).show()

# where() is same as filter()
df.where("Age > 23").show()

# Names starting with J
df.filter(df.Name.like("J%")).show()

# Sort by salary ascending
df.sort("Salary").show()

# Descending order
df.sort(df.Salary.desc()).show()

# Summary of numeric columns
df.describe().show()


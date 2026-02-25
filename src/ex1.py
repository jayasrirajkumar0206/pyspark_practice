# Import Spark

import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("DataFrameCollectExample").getOrCreate()

# Sample data
data = [("Jaya", 22), ("Rahul", 25), ("Priya", 23)]

# Column names
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
print("DataFrame Output:")
df.show()

# Collect data
print("Using collect():")
result = df.collect()

# Print collected data
for row in result:
    print(f"Name: {row['Name']}, Age: {row['Age']}")

# Stop Spark session
spark.stop()


input("Press Enter to stop Spark...")

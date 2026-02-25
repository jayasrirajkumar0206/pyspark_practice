from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"

spark = SparkSession.builder.appName("example").getOrCreate()

data = [("jay",20),("sri",22)]
columns=["Name","age"]
df = spark.createDataFrame(data,columns)

#register table for querying
df.createOrReplaceTempView("student")
result = spark.sql("select * from student where age > 20 ")

result.show()
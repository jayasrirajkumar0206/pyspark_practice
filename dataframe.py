from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


spark = SparkSession.builder.appName("Dataframe").getOrCreate()
data = [("jaya",20),("sri",20)]
df = spark.createDataFrame(data,["Name","age"])
df.show()
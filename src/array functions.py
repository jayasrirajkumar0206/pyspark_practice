from pyspark.sql import SparkSession
from pyspark.sql.functions import array
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("Practice1").getOrCreate()

data = [
    ("Jaya", "Math", "Science"),
    ("Ram", "English", "History"),
    ("Ravi", "Physics", "Chemistry")
]

df = spark.createDataFrame(data, ["Name", "Sub1", "Sub2"])

df.withColumn("Subjects", array("Sub1", "Sub2")).show(truncate=False)
#array_contains
from pyspark.sql.functions import array_contains

df2 = df.withColumn("Subjects", array("Sub1", "Sub2"))

df2.select("Name",
           array_contains("Subjects", "Math").alias("Has_Math")
).show()
#array_lenght
from pyspark.sql.functions import size

df2.select("Name",
           size("Subjects").alias("Total_Subjects")
).show()
#array_position
from pyspark.sql.functions import array_position

df2.select("Name",
           array_position("Subjects", "Science").alias("Science_Pos")
).show()
#array_remove
from pyspark.sql.functions import array_remove

df2.select("Name",
           array_remove("Subjects", "History").alias("Updated_Subjects")
).show(truncate=False)



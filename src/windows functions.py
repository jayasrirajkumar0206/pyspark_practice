from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("WindowPractice").getOrCreate()


data = [
    ("IT", "Jay", 5000, "2024-01-01"),
    ("IT", "Sri", 6000, "2024-02-01"),
    ("IT", "Ram", 6000, "2024-03-01"),
    ("HR", "Anu", 4000, "2024-01-15"),
    ("HR", "Kavi", 4500, "2024-02-15"),
    ("HR", "Divya", 4800, "2024-03-15")
]

columns = ["Dept", "Name", "Salary", "JoinDate"]

df = spark.createDataFrame(data, columns)
df.show()
#window specification
win = Window.partitionBy("Dept").orderBy(col("Salary").desc())

df.withColumn("row_number", row_number().over(win)).show()

df.withColumn("rank", rank().over(win)).show()

df.withColumn("dense_rank", dense_rank().over(win)).show()

df.withColumn("ntile_2", ntile(2).over(win)).show()

df.withColumn("next_salary", lead("Salary", 1).over(win)).show()

df.withColumn("prev_salary", lag("Salary", 1).over(win)).show()

df.withColumn("first_salary", first("Salary").over(win)).show()

win2 = Window.partitionBy("Dept").orderBy(col("Salary").desc()) \
             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn("last_salary", last("Salary").over(win2)).show()


df.withColumn("second_salary", nth_value("Salary", 2).over(win)).show()

df.withColumn("cume_dist", cume_dist().over(win)).show()

df.withColumn("percent_rank", percent_rank().over(win)).show()

#AVG
df.withColumn("avg_salary", avg("Salary").over(win)).show()
# SUM
df.withColumn("sum_salary", sum("Salary").over(win)).show()
# COUNT
df.withColumn("count_emp", count("Name").over(win)).show()
# MAX and MIN
df.withColumn("max_salary", max("Salary").over(win)).show()

df.withColumn("min_salary", min("Salary").over(win)).show()
#6. Window Frame (Moving Average)
# Moving Average Salary

win3 = Window.partitionBy("Dept").orderBy("Salary") \
             .rowsBetween(-1, 1)

df.withColumn("moving_avg", avg("Salary").over(win3)).show()
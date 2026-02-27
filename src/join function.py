from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
spark = SparkSession.builder.appName("JoinPractice").getOrCreate()

# Employee Data
emp_data = [
    (1, "Jaya", "IT"),
    (2, "Rahul", "HR"),
    (3, "Meena", "Finance"),
    (4, "Arun", "IT"),
    (5, "Kiran", "Admin")
]

emp = spark.createDataFrame(emp_data, ["EmpID", "Name", "Dept"])

# Department Data
dept_data = [
    ("IT", "Bangalore"),
    ("HR", "Chennai"),
    ("Admin", "Mumbai"),
    ("Sales", "Delhi")
]

dept = spark.createDataFrame(dept_data, ["Dept", "Location"])

print("Employee Table")
emp.show()

print("Department Table")
dept.show()

#inner joins-->Only matching records.
print("INNER JOIN")
emp.join(dept, "Dept", "inner").show()

#left joins-->Only matching records.all rows from left
print("LEFT JOIN")
emp.join(dept, "Dept", "left").show()

#RIGHT JOIN--> All rows from right + matching from left.
print("RIGHT JOIN")
emp.join(dept, "Dept", "right").show()

# FULL OUTER JOIN -->All records from both tables.
print("FULL OUTER JOIN")
emp.join(dept, "Dept", "outer").show()

#CROSS JOIN--> All combinations.
print("CROSS JOIN")
spark.conf.set("spark.sql.crossJoin.enabled", "true")
emp.crossJoin(dept).show()

#LEFT SEMI JOIN--->Only rows from left that match right.
print("LEFT SEMI JOIN")
emp.join(dept, "Dept", "left_semi").show()

#LEFT ANTI JOIN--->Rows from left that do NOT match.
print("LEFT ANTI JOIN")
emp.join(dept, "Dept", "left_anti").show()



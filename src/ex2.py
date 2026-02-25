import sys
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


from pyspark.sql import SparkSession


Spark = SparkSession.builder.master("local[4]").appName("Stages_Task").getOrCreate()

#print(Spark)
#print(Spark.sparkContext)

sc = Spark.sparkContext

rdd =sc.parallelize([(1,2,3,4),(5,6,7,8)],2) #2 partitions created

print("number of partitions :",rdd.getNumPartitions())

print("initial data structure",rdd.collect())

rdd1 = rdd.map(lambda x : (x[0],x[1]+1)) \
    .filter(lambda x: (x[1]>3))


print("narrow trasformation (map and filter) : ",rdd1.collect())


print(sc.uiWebUrl)
#print(sys.executable)

input("press enter to stop")
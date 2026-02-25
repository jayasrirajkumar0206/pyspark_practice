from pyspark import SparkContext

sc = SparkContext("local","Example")

data = [1,2,3,4,5]
rdd=sc.parallelize(data)
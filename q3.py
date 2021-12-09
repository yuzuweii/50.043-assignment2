import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as T
import ast

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.csv("hdfs://{}:9000/assignment2/part1/input/".format(hdfs_nn))

df = df.select(sf.col('ID_TA'), sf.col('Reviews'))

def tolist(string_list):
    list = ast.literal_eval(string_list)
    return list

# Using rdd apis here
df = df.rdd
df = df.map(lambda cols: ( cols[0],tolist(cols[1])[0], tolist(cols[1])[1] ))

df = df.toDF(["ID_TA", "review", "date"])

df = df.withColumn('tmp', sf.arrays_zip("review", "date"))\
    .withColumn('tmp', sf.explode('tmp'))\
    .select("ID_TA" , sf.col("tmp.review"), sf.col("tmp.date"))

df.show()
df.write.csv("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn), header=True)

import sys

# you may add more import if you need to
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, length, asc
from pyspark.sql.functions import split, regexp_replace
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StringType

from ast import literal_eval


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

# one-line read
df = spark.read\
    .option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.filter(length('Reviews') > 14)\
        .filter(col("Rating") >= 1.0) #  length > 14 => not null

df.groupBy('Reviews').agg({'Reviews': 'count'}).sort(desc("count(Reviews)")).show()

df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True) # output to HDFS folder

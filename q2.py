import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, length, asc, lit

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read\
    .option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.na.drop(subset=["Price Range"])
df.printSchema()

grouping = ["Price Range", "City"]

# Here we are using RDD apis
best = df.groupBy(grouping).agg({"Rating": "max"}).withColumn("Rating", col("max(Rating)"))
worst = df.groupBy(grouping).agg({"Rating": "min"}).withColumn("Rating", col("min(Rating)"))

unionDF = best.union(worst)

joinDF = unionDF.join(df, on=["Price Range", "City", "Rating"], how='inner')
joinDF = joinDF.sort(asc("City")).dropDuplicates(["Price Range", "City", "Rating"])

joinDF.show()
joinDF.write.csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), header=True)
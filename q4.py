import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc, length, asc, lit, explode
from pyspark.sql.types import StringType,BooleanType,ArrayType
# you may add more import if you need to

from pyspark.sql.functions import split, regexp_replace


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read\
    .option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))


df_exp = df.withColumn(
    "csstyle",
    split(regexp_replace("Cuisine Style", r"(^\[)|(\]$)", ""), ", ")
)
df_exp.printSchema()

df_exp = df_exp.select(df_exp.Name,df_exp.City,explode(df_exp.csstyle))


df_exp = df_exp.groupBy(["City", "col"]).agg(count("Name")).sort("City")
df_exp.show()
df_exp.write.csv("hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True)

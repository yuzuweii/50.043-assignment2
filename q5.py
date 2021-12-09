import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
import ast

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()

# YOUR CODE GOES BELOW

df = spark.read\
.option("header", True)\
.option("inferSchema", "true")\
.parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

# use rdd here
df2 = df.rdd


def extract_actors(line):
    casting = ast.literal_eval(line[2])
    out = []
    actors = []
    for i in range(len(casting)-1):
        for j in range(i+1,len(casting)):
            if casting[i]["name"] > casting[j]["name"]:
                actor = casting[i]["name"] + ',' + casting[j]["name"]
            else:
                actor = casting[j]["name"] + ',' + casting[i]["name"]
            if actor not in actors:
                actors.append(actor)
                out.append((str(line[0]) +','+line[1], actor) )
    return out

extracted_actors = df2.flatMap(extract_actors)

actorsCrossActors =extracted_actors.map(lambda line: (line[1], line[0]))

df3 = actorsCrossActors.map(lambda line: (line[0], 1)).reduceByKey(lambda x,y: x+y).filter(lambda line: line[1] >= 2).toDF()

# write CSV
df3.write.csv("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn), header=True)
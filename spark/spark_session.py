from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession\
        .builder\
        .appName("Sample")\
        .master("local")\
        .getOrCreate()

source = "file:///Users/imtaebin/Apps/spark/README.md"
df = spark.read.text(source)


wordDF = df.select(explode(split(col("value"), " ")).alias("word"))
result = wordDF.groupBy("word").count()
result.show()
# result.write().text("<path_to_save>")

spark.stop()

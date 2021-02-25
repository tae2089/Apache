from pyspark.sql import SparkSession


from pyspark import streaming

spark = SparkSession\
                .builder\
                .master("local")\
                .appName("sample")\
                .config("hive.metastore.uris", "thrift://localhost:10000")\
                .enableHiveSupport()\
                .getOrCreate()
spark.sql("show databases")

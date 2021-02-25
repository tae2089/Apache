from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

#spark 설정
conf = SparkConf()
sc = SparkContext(master="local[*]", appName="QueueSample", conf=conf)
ssc = StreamingContext(sc, 3)

#rdd생성
rdd1 = sc.parallelize(["a", "b", "c"])
rdd2 = sc.parallelize(["c", "d", "e"])
#que에 담기
queue = [rdd1, rdd2]

#queusStream데이터 생성
ds = ssc.queueStream(queue)

ds.pprint()

ssc.start()
ssc.awaitTermination()

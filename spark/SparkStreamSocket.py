from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

#sparkStream 생성
conf = SparkConf()
sc = SparkContext(master="local[*]", appName="SocketSample", conf=conf)
ssc = StreamingContext(sc, 3)

#소켓데이터 생성
ds = ssc.socketTextStream("localhost", 9000)
ds.pprint()

#sparkStream 생성에 필요한 것들
ssc.start()
ssc.awaitTermination()

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
import collections

def main():
    spark = SparkSession.builder \
    .appName("sample") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///Users/beginspark/Temp/") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

    sc = spark.sparkContext
    # 파이썬에서 데이터프레임 생성 시 네임드튜플(namedtuple), 튜플(tuple)
    # Row, 커스텀 클래스(class), 딕셔너리(dictionary) 등을
    # 사용하여 생성할 수 있다
    Person = collections.namedtuple('Person', 'name age job')

    # sample dataframe 1
    row1 = Person(name="hayoon", age=7, job="student")
    row2 = Person(name="sunwoo", age=13, job="student")
    row3 = Person(name="hajoo", age=5, job="kindergartener")
    row4 = Person(name="jinwoo", age=13, job="student")
    data = [row1, row2, row3, row4]
    sample_df = spark.createDataFrame(data)

    d1 = ("store2", "note", 20, 2000)
    d2 = ("store2", "bag", 10, 5000)
    d3 = ("store1", "note", 15, 1000)
    d4 = ("store1", "pen", 20, 5000)
    sample_df2 = spark.createDataFrame([d1, d2, d3, d4]).toDF("store", "product", "amount", "price")

    ldf = spark.createDataFrame([Word("w1", 1), Word("w2", 1)])
    rdf = spark.createDataFrame([Word("w1", 1), Word("w3", 1)])

if __name__ == '__main__':
    main()

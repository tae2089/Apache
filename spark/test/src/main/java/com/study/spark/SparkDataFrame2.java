package com.study.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class SparkDataFrame2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").config(conf).getOrCreate();
        
        //컬럼 만들기
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("NAME", DataTypes.StringType, false), 
                DataTypes.createStructField("AGE", DataTypes.IntegerType, false)
        });
        //데이터 담을 ROW List만들기
        List<Row> rows = new ArrayList<>();
        //ROW를 만들때에는 RowFactory를 사용해서 만들면 된다.
        Row r1 = RowFactory.create("name1", 1);
        Row r2 = RowFactory.create("name2", 2);
        rows.add(r1);
        rows.add(r2);


        Dataset<Row> ds = spark.createDataFrame(rows, schema);
        ds.show();
        
        //datatype 변형해보기 및 컬럼 추가해보기
        ds.withColumn("Test", ds.col("AGE").cast(DataTypes.DoubleType)).show(false);

        //withColumn을 사용해서 바꿔보기
        ds.withColumn("Test", ds.col("AGE").plus(1)).show(false);
        //select
        ds.select(ds.col("AGE").$greater$eq(1)).show();
    }
}

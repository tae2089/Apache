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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@SuppressWarnings("serial")
public class SparkDataFrameUdf {
    
    static public UDF1<Integer, Integer> sampleUDf = new UDF1<Integer, Integer>() {
        @Override
        public Integer call(Integer t1) throws Exception {

            return t1 + 10;
        }
    };
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").config(conf).getOrCreate();

        // 컬럼 만들기
        StructType schema = DataTypes
                .createStructType(new StructField[] { DataTypes.createStructField("NAME", DataTypes.StringType, false),
                        DataTypes.createStructField("AGE", DataTypes.IntegerType, false) });
        // 데이터 담을 ROW List만들기
        List<Row> rows = new ArrayList<>();
        // ROW를 만들때에는 RowFactory를 사용해서 만들면 된다.
        Row r1 = RowFactory.create("name1", 1);
        Row r2 = RowFactory.create("name2", 2);
        rows.add(r1);
        rows.add(r2);

        Dataset<Row> ds = spark.createDataFrame(rows, schema);
        // udf만들어보기
        // labmda 형식
        UserDefinedFunction udfUppercase = udf((String s) -> s.toUpperCase(), DataTypes.StringType);
        ds.withColumn("test", udfUppercase.apply(ds.col("NAME"))).show();
        // udf함수를 만들어서 넣어주기
        UserDefinedFunction udfUppercase2 = udf(sampleUDf, DataTypes.IntegerType);
        ds.withColumn("test", udfUppercase2.apply(ds.col("AGE"))).show();
    }
}

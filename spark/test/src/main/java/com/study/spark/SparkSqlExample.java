package com.study.spark;

import com.study.spark.domain.People;
import com.study.spark.domain.Person;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").config(conf).getOrCreate();
        Dataset<Row> df = spark.read().json("resource/people.json");
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.printSchema();
        sqlDF.show();

        try {
            df.createGlobalTempView("people");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        spark.sql("SELECT * FROM global_temp.people").show();
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

        Encoder<People> personEncoder = Encoders.bean(People.class);
        Dataset<People> peopleDS = spark.sql("SELECT * FROM people").as(personEncoder);
        peopleDS.show();
    }
}

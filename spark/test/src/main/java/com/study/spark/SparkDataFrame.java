package com.study.spark;

import java.util.ArrayList;
import java.util.List;

import com.study.spark.domain.Person;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataFrame {
    public static void main(String [] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config(conf).getOrCreate();

        Dataset<Row> df = spark.read().json("./resource/people.json");
        df.show();
        df.printSchema();

        List<Person> personlist = new ArrayList<>();
        Person person1 = Person.builder().age(11).name("ssss").build();
        Person person2 = Person.builder().age(12).name("youjong").build();
        personlist.add(person1);
        personlist.add(person2);


    }
}

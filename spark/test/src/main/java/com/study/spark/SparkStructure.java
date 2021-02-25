package com.study.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.study.spark.domain.Person;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkStructure {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        
        Person person2 = new Person();
        person2.setName("솔");
        person2.setAge(31);

        List<Person> personList = new ArrayList<Person>();
        personList.add(person);
        personList.add(person2);
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(personList, personEncoder);
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.show();
    }
}

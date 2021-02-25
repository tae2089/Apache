package com.study.spark;


import java.util.Arrays;


import com.study.spark.method.SparkRdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class App {
    public static void main(String[] args) {
        SparkRdd spark = new SparkRdd();
        JavaSparkContext sc = spark.SparkContext();
        
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<String, Integer> rdd4 = spark.rddMapValues(rdd2);
        System.out.println(rdd4.collect());
    }
}

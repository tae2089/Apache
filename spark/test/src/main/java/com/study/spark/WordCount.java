package com.study.spark;


import com.study.spark.method.SparkRdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;



public class WordCount{
    public static void main(String [] arg){
        //spark 선언
        SparkRdd spark = new SparkRdd();
        JavaSparkContext sc = spark.SparkContext();
        
        JavaRDD<String> word = sc.textFile("/Users/hello/Documents/Codes/Data_Study/Code/test.txt");
        JavaRDD<String> word_replace_coma = word.map((String t1) -> t1.replace(",",""));
        JavaRDD<String> word_remove_space = spark.rddFlatMap(word_replace_coma);
        JavaPairRDD<String,Integer> word_count_one = spark.rddMapValues(word_remove_space);
        JavaPairRDD<String,Integer> word_count = word_count_one.reduceByKey((Integer a, Integer b) -> a+b);
        System.out.println(word_count.collect());

    }
}

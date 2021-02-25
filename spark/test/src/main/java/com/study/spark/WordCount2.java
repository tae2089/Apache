package com.study.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import scala.Tuple2;

@SuppressWarnings("serial")
public class WordCount2 {
    public static void main(String[] args){
        // log 제어 코드
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // sparkContext생성하기
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 1), new Tuple2<String, Integer>("c", 1),
                new Tuple2<String, Integer>("a", 2), new Tuple2<String, Integer>("c", 1));

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(new Function<Iterable<Integer>, Integer>() {

            @Override
            public Integer call(Iterable<Integer> v1) throws Exception {
                        int result = 0;
            for (Integer item : v1)
                result += item;
            return result;
            }
        });
        System.out.println(rdd3.collect());
        sc.close();
    }
}

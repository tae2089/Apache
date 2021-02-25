package com.study.spark.method;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkRdd implements Serializable {

    private static final long serialVersionUID = 1L;

    public JavaSparkContext SparkContext() {
        // log 제어 코드
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // sparkContext생성하기
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        return sc;
    }

    public JavaRDD<Integer> rddMap(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> rdd2 = rdd.map(v1 -> v1 + 1);
        return rdd2;
    }

    public JavaRDD<String> rddFlatMap(JavaRDD<String> rdd) {
        JavaRDD<String> rdd2 = rdd.flatMap((String t)  -> Arrays.asList(t.split(" ")).iterator());
        return rdd2;
    }

    public JavaRDD<Integer> rddMapPartion(JavaRDD<Integer> rdd){
        JavaRDD<Integer> rdd3 = rdd.mapPartitions((Iterator<Integer> numbers) -> {
            System.out.println("DB연결 !!!");
            List<Integer> result = new ArrayList<>();
            numbers.forEachRemaining(i -> result.add(i + 1));
            return result.iterator();
        });
        return rdd3;
    }

    public JavaPairRDD<String, Integer> rddMapValues(JavaRDD<String> rdd){
        JavaPairRDD<String, Integer> result = rdd.mapToPair((String t) -> new Tuple2<String,Integer>(t,1));
        return result;
    }

}

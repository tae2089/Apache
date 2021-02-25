package com.study.spark;


import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

@SuppressWarnings("serial")
public class SparkCombine{
//처음 값이 들어왔을때 integer를 반환
static Function<Integer,Integer> combiner = new Function<Integer,Integer>(){
    @Override
    public Integer call(Integer v1) throws Exception {
        return v1;
    }
};
// 이전의 값이 들어왔던 적이 있다면 값을 더하고 반환
static Function2 <Integer,Integer,Integer> merge = new Function2<Integer, Integer,Integer>(){
    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1+v2;
    }
};

    public static void main(String[] args) {
        // log 제어 코드
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // sparkContext생성하기
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<String, Integer>("a", 1), 
            new Tuple2<String, Integer>("b", 1), 
            new Tuple2<String, Integer>("c", 1),
            new Tuple2<String, Integer>("a", 2), 
            new Tuple2<String, Integer>("c", 1));

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd2 = rdd1.combineByKey(
        (t2) -> new Tuple2<Integer,Integer>(t2,1),
        (tuple0, num) -> new Tuple2<Integer, Integer>(tuple0._1() + num, tuple0._2 + 1),
        (tuple1, tuple2) -> new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(tuple -> tuple._2());
        System.out.println(rdd3.collect());
        
        JavaPairRDD<String, Integer> rdd4 = sc.parallelizePairs(data);
        JavaPairRDD<String, Integer> rdd5 = rdd4.combineByKey(
                (Integer v1)-> v1 ,
                (Integer v1, Integer v2) -> v1+v2,
                merge);
        System.out.println(rdd5.collect());
        sc.close();
    }
}

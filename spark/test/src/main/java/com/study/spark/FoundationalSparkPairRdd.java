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
public class FoundationalSparkPairRdd {
    public static void main(String[] args) {
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
        // groupbykey
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
        // mapValues
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

        //reducebykey
        List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 1), new Tuple2<String, Integer>("c", 1),
                new Tuple2<String, Integer>("a", 1), new Tuple2<String, Integer>("c", 1));

        JavaPairRDD<String, Integer> rdd5 = sc.parallelizePairs(data1);
        JavaPairRDD<String, Integer> reducebykeyrdd = rdd5.reduceByKey(new Function2<Integer, Integer,Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}});
        System.out.println(reducebykeyrdd.collect());

        // aggregateByKey
        JavaPairRDD<String, Integer> rdd6 = sc.parallelizePairs(data1,2);
            JavaPairRDD<String, String> aggregateByKeyRdd = rdd6.aggregateByKey("*", new Function2<String,Integer,String>() {
				@Override
				public String call(String v1, Integer v2) throws Exception {
					return v1+"#"+String.valueOf(v2);
				}}, new Function2<String,String,String>(){

                @Override
                public String call(String v1, String v2) throws Exception {
                    return v1+"$"+v2;
                }
            });
            System.out.println(aggregateByKeyRdd.collect());
        sc.close();
    }
}

package com.study.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

@SuppressWarnings("serial")
public class FoundationalRdd {
    public static void main(String[] args) {
        // spark 설정하기
        // log 제어 코드
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // sparkContext생성하기
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // map
        List<Integer> a = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> IntegerRdd = sc.parallelize(a);
        JavaRDD<Integer> MapRdd = IntegerRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 + 1;
            }
        });

        System.out.println("Map -> " + MapRdd.collect());
        // reduce
        int ReduceRdd = IntegerRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("reduce -> " + ReduceRdd);
        
        // filter
        JavaRDD<Integer> FilterRdd = IntegerRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                if (v1 % 2 == 0) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        System.out.println("Filter ->" + FilterRdd.collect());

        // foreach
        FilterRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t1) throws Exception {
                System.out.println(t1);
            }
        });
        //pipeline
        List<Integer> PipelineRdd = IntegerRdd
                                                        .map(v1 -> v1+2)
                                                        .filter(v1 -> {if (v1 % 2 == 0) return true; 
                                                                                else return false;})
                                                        .collect();
        System.out.println(PipelineRdd);
        sc.close();
    }
}

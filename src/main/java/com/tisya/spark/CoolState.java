package com.tisya.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CoolState {
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Cool State");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> citiesTemp = sc.textFile("cities.txt");
        JavaRDD<String[]> data = citiesTemp.map(x -> x.split(","));
        JavaPairRDD<String, Integer> stateTemp = data.mapToPair(x -> new
                Tuple2<String, Integer>(x[3], Integer.parseInt(x[5])));
        Tuple2<Integer, String> coolState =
                stateTemp.groupByKey().mapToPair(t -> {
                    List<Integer> list = new ArrayList<>();
                    t._2.forEach(list::add);
                    return new Tuple2<Integer,
                            String>(Collections.min(list), t._1);
                }).sortByKey().first();
        System.out.println("Coolest State is " + coolState._2 + " : " + coolState._1);
        sc.close();
    }
}

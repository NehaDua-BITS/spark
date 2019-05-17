package com.tisya.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
public class HelloSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
             .setMaster("local")
                     .setAppName("Hello Spark");
                     JavaSparkContext sc = new JavaSparkContext(sparkConf);
                     System.out.println("Hello from Spark Application - "
                     + sc.appName());
                     sc.close();
    }
}
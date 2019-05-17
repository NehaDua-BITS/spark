package com.tisya.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class BasicTransformations {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Basic Transformations - Spark");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        System.out.println("Basic transformations program");

        Integer[] arr1 = {1,2,3,4,4,5,6,7,7,9};
        Integer[] arr2 = {4,5,6,7};

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(arr1));
        //adding 2
        JavaRDD<Integer> sumRdd = rdd1.map(num -> num+2);
        System.out.println(sumRdd.collect());

        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(arr2));
        JavaRDD<Integer> oddNums = rdd2.filter(x -> (x%2!=0));
        File file = new File("rdd-content.txt");
        new ObjectMapper().writeValue(file, oddNums);  //writing rdd to file
        oddNums.saveAsTextFile("oddnumrdd.txt");
        System.out.println("Count of odd num : " + oddNums.count());
        System.out.println("Odd numbers : " + oddNums.collect());

        sc.close();
    }
}

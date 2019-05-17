package com.tisya.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopTenPopularWords
{
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Top 10 Popular Words");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sc.textFile("story.txt");
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        List<Tuple2<Integer, String>> words = wordsFromFile.filter(word -> word.length() > 3)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x+y)
                .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                .sortByKey(false).take(10);

        System.out.println("Top 10 words are : ");
        words.forEach(t -> System.out.println(t._2 + " " + t._1));

        sc.close();
    }
}

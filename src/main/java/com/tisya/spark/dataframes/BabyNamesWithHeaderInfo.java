package com.tisya.spark.dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class BabyNamesWithHeaderInfo
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .appName("Baby names with header info")
                .master("local")
                .getOrCreate();

        Dataset<Row> babyNamesDF = spark.read().option("header", "true").option("inferSchema", "true").csv("baby_names.csv");
        System.out.println("baby names data is : ");
        babyNamesDF.show();
        System.out.println("baby names schema is : ");
        babyNamesDF.printSchema();

        List<Row> rows = babyNamesDF.groupBy(col("FirstName")).sum("Count").sort(col("sum(Count)").desc()).takeAsList(10);
        rows.forEach(row -> System.out.println(row.get(0) + " : " + row.get(1)));
    }
}

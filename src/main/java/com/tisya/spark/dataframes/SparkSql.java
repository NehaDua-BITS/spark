package com.tisya.spark.dataframes;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

/** Spark SQL and Data frame operations **/
public class SparkSql
{
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL App")
                .config("spark.master", "local")   //use this or .master ; both are fine for setting local
                .getOrCreate();

        Dataset<Row> peopleDF = spark.read().json("people.json");
        peopleDF.createTempView("people");
        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> allPeople = sqlContext.sql("SELECT name, address.city, address.state FROM people");
        allPeople.foreach(row -> System.out.printf(row.getString(0) + "\n"));

        //System.out.println(allPeople.toJSON().collect());
        //System.out.println(allPeople.toJSON().head());
        System.out.println("All people are :");
        allPeople.toJSON().foreach(json -> System.out.println(json));

        Dataset<Row> caPeople = sqlContext.sql("SELECT name, address.city, address.state FROM people where address.state='California'");
        System.out.println("California people are : ");
        caPeople.toJSON().foreach(json -> System.out.println(json));

        System.out.println("Showing dataframe :");
        peopleDF.show();

        System.out.println("Count of people :"+ peopleDF.count());
        peopleDF.printSchema();  //prints schema in tree format

        peopleDF.select("name").show();
        peopleDF.select("address").show();
        peopleDF.select(col("name"), col("age").plus(1)).show();

        //peopleDF.filter("5 < 4").show();

        peopleDF.filter(col("age").gt(25)).show();
        peopleDF.groupBy(col("age")).count().show();
        peopleDF.groupBy("age").count().show();

        spark.close();
    }

}

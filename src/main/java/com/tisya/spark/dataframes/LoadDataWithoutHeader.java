package com.tisya.spark.dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class LoadDataWithoutHeader
{
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Hottest state")
                .getOrCreate();

        //Original data that has no headers/schema
        Dataset<Row> cities = spark.read().csv("cities.txt");
        cities.printSchema();
        cities.createOrReplaceTempView("cities");
        spark.sql("select _c0, _c1 from cities").show();

        //Assigning schema to the data
        StructType citiesSchema = new StructType(
                new StructField[] { DataTypes.createStructField("LONGITUDE", DataTypes.IntegerType, true),
                        DataTypes.createStructField("LATITUDE", DataTypes.IntegerType, true),
                        DataTypes.createStructField("CITY", DataTypes.StringType, true),
                        DataTypes.createStructField("STATE", DataTypes.StringType, true),
                        DataTypes.createStructField("MAX_TEMP", DataTypes.IntegerType, true),
                        DataTypes.createStructField("MIN_TEMP", DataTypes.IntegerType, true)
                });
        Dataset<Row> citiesDF = spark.read().option("header", "false").schema(citiesSchema).csv("cities.txt");
        citiesDF.printSchema();
        citiesDF.show();
        citiesDF.createOrReplaceTempView("citiesDFView");

        spark.sql("Select * from citiesDFView where MAX_TEMP in (select max(MAX_TEMP) from citiesDFView)").show();

        Row hottest = citiesDF.groupBy(col("STATE")).max("MAX_TEMP")
                                .withColumnRenamed("max(MAX_TEMP)", "temp")
                                .sort(col("temp").desc()).first();
        System.out.println("State : " + hottest.getString(0) + " Temp : " + hottest.getInt(1));
    }
}

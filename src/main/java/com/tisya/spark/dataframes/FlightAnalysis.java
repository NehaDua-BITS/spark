package com.tisya.spark.dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FlightAnalysis
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Flight analysis using Data Frame")
                .getOrCreate();

        Dataset<Row> airports = spark.read().option("sep", "\t").option("header", "true").option("inferSchema", "true").csv("airport-codes-na.txt");
        airports.show();
        airports.createOrReplaceTempView("airports");
        spark.sql("Select distinct state from airports").show();

        Dataset<Row> departureDelays = spark.read().option("header", "true").option("inferSchema", "true").csv("departuredelays.csv");
        departureDelays.show();
        departureDelays.createOrReplaceTempView("departureDelays");
        Dataset<Row> rows = spark.sql("Select delay,origin from departureDelays");
        rows.show();

        spark.sql("select a.City, f.origin, sum(f.delay) as Delays from departureDelays f join airports a on a.IATA = f.origin where a.State = 'WA' group by a.City, f.origin order by sum(f.delay) desc").show();
        spark.sql("select a.State, sum(f.delay) as Delays from departureDelays f join airports a on a.IATA = f.origin where a.Country = 'USA' group by a.State").show();

        departureDelays.printSchema();   //it will infer correct schema if option is set as true while reading
        airports.printSchema();

        spark.close();
    }
}

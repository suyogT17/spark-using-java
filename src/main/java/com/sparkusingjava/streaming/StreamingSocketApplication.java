package com.sparkusingjava.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StreamingSocketApplication {

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Streaming Socket")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();


        String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
                "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
                "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
                "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
                "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
                "'your', 'you', 'I', "
                + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";

        Dataset<Row> df = sparkSession.readStream()
                                        .format("socket")
                                        .option("host","localhost")
                                        .option("port",4041)
                                        .load();


        Dataset<String> ds = df.flatMap((Row row)-> Arrays.asList(row.toString().toLowerCase().replaceAll("[^a-zA-z\\s]","").split(" ")).iterator(),Encoders.STRING());


        df = ds.toDF();
        df = df.filter(value -> !boringWords.contains(value.getString(0)));
        df = df.groupBy("value").count().orderBy(functions.col("count").desc());
        StreamingQuery query = df.writeStream().outputMode("complete").format("console").start();

        query.awaitTermination();
    }
}



package com.sparkusingjava.applications;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class RedditApplication {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
                "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
                "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
                "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
                "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
                "'your', 'you', 'I', "
                + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";

        SparkSession sparkSession = SparkSession.builder()
                .appName("Reddit Comments Analysis")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();


        String redditfile = "src/main/resources/resource6/RC_2007-01";

        Dataset<Row> redditDf = sparkSession.read().format("json")
             //   .option("multiline",true) // Make sure to use string version of true
                .option("header", true)
                .load(redditfile);
        //redditDf.printSchema();
        Dataset<String> commentsDs = redditDf.select(col("body"))
                                            .flatMap(row -> Arrays.asList(row.toString().trim().replaceAll("[^a-zA-z\\s]","").trim().split(" ")).iterator(), Encoders.STRING())
                                            .filter("value not in "+boringWords).filter(value -> value.length() > 0);
        Dataset<Row> commentsDf = commentsDs.toDF().groupBy("value").count().orderBy(col("count").desc());
        commentsDf.show();
    }

}

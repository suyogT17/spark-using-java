package com.sparkusingjava.helper;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;


public class WordCount {

	public void start() {

		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		Logger logger = Logger.getLogger("org.apache");
		logger.setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder()
				.appName("Word Count")
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

		String filename = "src/main/resources/resource4/shakespeare.txt";

		Dataset<Row> df = sparkSession.read().format("text")
				.load(filename);

		//Dataset<String> ds = df.flatMap((Row row)-> Arrays.asList(row.toString().toLowerCase().replaceAll("[^a-zA-z\\s]","").split(" ")).iterator(),Encoders.STRING());

		//alternet way without lambda expression
		Dataset<String> ds = df.flatMap(new LineMapper(),Encoders.STRING());

		df = ds.toDF();
		df = df.filter(value -> !boringWords.contains(value.getString(0)));
		df = df.groupBy("value").count().orderBy(functions.col("count").desc());
		df.show();
		df.explain();
	}


}

class LineMapper implements FlatMapFunction<Row,String> , Serializable {
	@Override
	public Iterator<String> call(Row row) throws Exception {
		return Arrays.asList(row.toString().toLowerCase().replaceAll("[^a-zA-z\\s]","").split(" ")).iterator();

	}
}
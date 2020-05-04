package com.sparkusingjava.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class StreamingFileDirectoryApplication {
	
	public static void main(String[] args) throws StreamingQueryException {


		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		Logger logger = Logger.getLogger("org.apache");
		logger.setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder()
				.appName("Streaming File Directory")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
				.getOrCreate();
		
		// Read all the csv files written atomically in a directory
		StructType userSchema = new StructType().add("date", "string").add("value", "float");
		
		Dataset<Row> stockData = sparkSession
		  .readStream()
		  .option("sep", ",")
		  .schema(userSchema)      // Specify schema of the csv files
		  .csv("src/main/resources/Resource7/IncomingStockFiles"); // Equivalent to format("csv").load("/path/to/directory")
		
		
		Dataset<Row> resultDf = stockData.groupBy("date").agg(avg(stockData.col("value")));
		
		StreamingQuery query = resultDf.writeStream()
				.outputMode("complete")
				.format("console")
				.start();
		
		query.awaitTermination();
		
	}
		
		
}

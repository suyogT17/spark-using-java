package com.sparkusingjava.helper;

import com.sparkusingjava.pojo.House;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;

public class CsvToDatasetHouseToDataframe {
	
	public void start() {


		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		Logger logger = Logger.getLogger("org.apache");
		logger.setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder()
				.appName("Combine Datasets")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
				.getOrCreate();
		
		
		 String filename = "src/main/resources/resource4/houses.csv";
		 
		    Dataset<Row> df = sparkSession.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .option("sep", ";")
		        .load(filename);

		    Dataset<House> houseDs = df.map((Row row)->{
		    	House house = new House();
		    	house.setId(row.getAs("id"));
				house.setAddress(row.getAs("address"));
				house.setSqft(row.getAs("sqft"));
				house.setPrice(row.getAs("price"));
				String vacancyDateString = row.getAs("vacantBy").toString();
				if(vacancyDateString != null)
				{
					SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
					house.setVacantBy(parser.parse(vacancyDateString));
				}
				return house;
			},Encoders.bean(House.class));

		    houseDs.show();

		    Dataset<Row> houseDf = houseDs.toDF();
		    houseDf.show();
	}
	

	    
}

 
   
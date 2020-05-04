package com.sparkusingjava.helper;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {

    public  void  start(){

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Combine Datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();

        String [] stringList = {"Banana","Car","Glass","Banana","Computer","Car"};

        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = sparkSession.createDataset(data, Encoders.STRING());

        /*ds.printSchema();
        Dataset<Row>  df1 = ds.toDF(); //dataset to dataframe
        df1.show();

        ds = df1.as(Encoders.STRING()); // dataframe to dataset
        Dataset<Row> df2 = ds.groupBy("value").count(); //dataset to dataframe
        df2.show();*/

        ds = ds.map(value-> value+"12",Encoders.STRING());
        ds.show();
        String finalop = ds.reduce((value1,value2)-> value1+value2);
        System.out.println(finalop);
    }

}

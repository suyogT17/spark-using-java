package com.sparkusingjava.applications;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class UnionDataFrames {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                                        .appName("Combine Datasets")
                                        .master("local[*]")
                                        .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                                        .getOrCreate();

        Dataset<Row> parkCsvDF =  buildPhiladelphiaDataFrame(sparkSession);

        Dataset<Row> parkJsonDF =  buildDurhamParksDataFrame(sparkSession);

        //parkCsvDF.printSchema();
        //parkCsvDF.show(5,250);
        //parkJsonDF.printSchema();
        //parkJsonDF.show(5,250);

        Dataset<Row> unionDF = combineDataFrames(parkCsvDF,parkJsonDF);
        /*unionDF.show();
        System.out.println("Total Row Count: "+unionDF.count());
        unionDF = unionDF.repartition(4);
        Partition partition[] = unionDF.rdd().partitions();
        System.out.println("Total Partitions: "+partition.length);
        */
        parkCsvDF.createOrReplaceTempView("philadelphia");
        parkJsonDF.createOrReplaceTempView("durham");

        Dataset<Row> sqlUnion = sparkSession.sql("select park_id,park_name, address, land_in_acres, zipcode, city, geoX,geoY, has_playground from philadelphia union select park_id,park_name, address, land_in_acres, zipcode, city, geoX,geoY, has_playground from durham");
        sqlUnion.show();
        System.out.println("Total Row Count: "+sqlUnion.count());



    }

    private static Dataset<Row> combineDataFrames(Dataset<Row> parkCsvDF, Dataset<Row> parkJsonDF) {

        return parkCsvDF.unionByName(parkJsonDF);
    }

    static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession){
        Dataset<Row> parkJsonDF =  sparkSession.read().format("json")
                .option("header",true)
                .option("multiline", true)
                .load("src/main/resources/resource3/durham-parks.json");
        parkJsonDF = parkJsonDF.withColumn("park_id", concat(col("datasetid"),lit("_"),col("fields.objectid"),lit("_Durhum")))
                                .withColumn("park_name",col("fields.park_name"))
                                .withColumn("city",lit("durham"))
                                .withColumn("address",col("fields.address"))
                                .withColumn("has_playground",col("fields.playground"))
                                .withColumn("zipcode",col("fields.zip"))
                                .withColumn("land_in_acres",col("fields.acres"))
                                .withColumn("geoX",col("geometry.coordinates").getItem(0))
                                .withColumn("geoY",col("geometry.coordinates").getItem(1))
                                .drop("datasetid","fields","geometry","record_timestamp","recordid");
        return parkJsonDF;
    }

   static Dataset<Row> buildPhiladelphiaDataFrame(SparkSession sparkSession){

        Dataset<Row> parkCsvDF =  sparkSession.read().format("csv")
                .option("header",true)
                .load("src/main/resources/resource3/philadelphia_recreations.csv");

        parkCsvDF = parkCsvDF.filter(lower(col("USE_")).like("%park%"))
                                .withColumn("park_id",concat(col("OBJECTID"),lit("_Philadelphia")))
                                .withColumnRenamed("ASSET_NAME","park_name")
                                .withColumn("city",lit("philadelphia"))
                                .withColumnRenamed("ADDRESS","address")
                                .withColumn("has_playground",lit("UNKNOWN"))
                                .withColumnRenamed("ZIPCODE","zipcode").na().fill(00000)
                                .withColumnRenamed("ACREAGE","land_in_acres")
                                .withColumn("geoX",lit("UNLNOWN"))
                                .withColumn("geoY",lit("UNKNOWN"))
                                .drop("OBJECTID","SITE_NAME","CHILD_OF","TYPE","USE_","DESCRIPTION","SQ_FEET","ALLIAS","CHRONOLOGY",
                                        "NOTES","DATE_EDITED","EDITED_BY","OCCUPANT","TENANT","LABEL");
        return  parkCsvDF;
    }

}

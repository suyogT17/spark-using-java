package com.sparkusingjava.ML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeansDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Logistic Regression")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();

        Dataset<Row> wholesaleDf = sparkSession.read()
                .format("csv")
                .option("header",true)
                .option("inferSchema",true)
                .load("src/main/resources/resource8/Kmeans.csv");

        Dataset<Row> featuresDf = wholesaleDf.select("Channel","Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen");
        String featuresCol[] = {"Channel","Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen"};
        VectorAssembler assembler =  new VectorAssembler()
                .setInputCols(featuresCol)
                .setOutputCol("features");

        Dataset<Row> trainingData = assembler.transform(featuresDf).select("features");
        KMeans kMeans = new KMeans();
        kMeans.setK(3);
        KMeansModel kMeansModel = kMeans.fit(trainingData);
        Dataset<Row> result = kMeansModel.summary().predictions();
        System.out.println(kMeansModel.computeCost(trainingData));
        result.show(false);
    }

}

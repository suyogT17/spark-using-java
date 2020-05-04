package com.sparkusingjava.ML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.VectorAssembler;


public class LinearRegressionDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Logistic Regression")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();

        Dataset<Row> salesVsMarketingDf = sparkSession.read()
                                                        .format("csv")
                                                        .option("header",true)
                                                        .option("inferSchema",true)
                                                        .load("src/main/resources/resource8/LinearRegression.csv");
        //label should be the first column and feature(array of other column values)
        Dataset<Row> mlDf = salesVsMarketingDf.withColumnRenamed("sales","label")
                                                .select("label","marketing_spend");


        String[] featureColumnss = {"marketing_spend"};

        VectorAssembler assembler = new VectorAssembler().setInputCols(featureColumnss).setOutputCol("features");
        Dataset<Row> labelFeatureDf = assembler.transform(mlDf).select("label","features");
        labelFeatureDf.show();

        //Linear Regression

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel linearRegressionModel = linearRegression.fit(labelFeatureDf);
        linearRegressionModel.summary().predictions().show();

        //salesVsMarketingDf.show();

    }

}

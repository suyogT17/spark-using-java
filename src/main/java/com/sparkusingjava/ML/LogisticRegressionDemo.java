package com.sparkusingjava.ML;

import com.sun.research.ws.wadl.Param;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Logistic Regression")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();

        Dataset<Row> cryotherapyDf = sparkSession.read()
                .format("csv")
                .option("header",true)
                .option("inferSchema",true)
                .load("src/main/resources/resource8/LogisticRegression.csv");


        Dataset<Row> labelFeaturesDF = cryotherapyDf.withColumnRenamed("Result_of_Treatment","label")
                .select("label","sex","age","Time","Number_of_Warts","Type","Area")
                .na().drop();

        StringIndexer genderIndexer = new StringIndexer()
                                .setInputCol("sex")
                                .setOutputCol("sexid");
        String[] featureCols = {"sexid","age","Time","Number_of_Warts","Type","Area"};
        VectorAssembler assembler = new VectorAssembler()
                                    .setInputCols(featureCols)
                                    .setOutputCol("features");
        Dataset<Row>[] splitData = labelFeaturesDF.randomSplit(new double[]{.7,.3});
        Dataset<Row> trainingDf = splitData[0];
        Dataset<Row> testingDf = splitData[1];

        LogisticRegression logisticRegression = new LogisticRegression();

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{genderIndexer,assembler,logisticRegression});

        PipelineModel  pipelineModel = pipeline.fit(trainingDf);
        Dataset<Row> result = pipelineModel.transform(testingDf);
        result.show();
        System.out.println(pipelineModel);
    }

}

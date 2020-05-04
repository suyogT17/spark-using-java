package com.sparkusingjava.applications;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class JoinApplication {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Combine Datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();


        String studentsFile = "src/main/resources/resource5/students.csv";

        Dataset<Row> studentDf = sparkSession.read().format("csv")
                .option("inferSchema",true) // Make sure to use string version of true
                .option("header", true)
                .load(studentsFile);

        String gradeChartFile = "src/main/resources/resource5/grade_chart.csv";

        Dataset<Row> gradesDf = sparkSession.read().format("csv")
                .option("inferSchema", true) // Make sure to use string version of true
                .option("header", true)
                .load(gradeChartFile);

        Dataset<Row> finalResult = studentDf.join(gradesDf,studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
                //.filter(studentDf.col("gpa").between(2,4))
                .where(studentDf.col("gpa").between(2,4))
                .select(col("student_name"),col("favorite_book_title"),col("letter_grade"));// we can refer column using only col until they are not ambiguous
        finalResult.show();

    }
}


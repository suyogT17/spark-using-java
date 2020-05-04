package com.sparkusingjava.applications;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger logger = Logger.getLogger("org.apache");
        logger.setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Combine Datasets")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///D:/Suyog Work/Study/Spark/tmp") //only for windows
                .getOrCreate();



        String customersFile = "src/main/resources/resource5/customers.csv";

        Dataset<Row> customersDf = sparkSession.read().format("csv")
                .option("inferSchema",true) // Make sure to use string version of true
                .option("header", true)
                .load(customersFile);

        String productsFile = "src/main/resources/resource5/products.csv";

        Dataset<Row> productsDf = sparkSession.read().format("csv")
                .option("inferSchema",true) // Make sure to use string version of true
                .option("header", true)
                .load(productsFile);

        String purchasesFile = "src/main/resources/resource5/purchases.csv";

        Dataset<Row> purchasesDf = sparkSession.read().format("csv")
                .option("inferSchema",true) // Make sure to use string version of true
                .option("header", true)
                .load(purchasesFile);


        sparkSession.udf().register("productPriceConcat",(String product_name,Double price)-> product_name+" : "+price, DataTypes.StringType);

        Dataset<Row> finalDf = purchasesDf.join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
                                            .join(customersDf,customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
                                            .drop(customersDf.col("customer_id"))
                                            .drop(productsDf.col("product_id"))
                                            .drop(col("product_id"))
                                            .drop(col("favorite_website"));

        Dataset<Row> customerPurchaseDf = finalDf.groupBy("customer_id","last_name","first_name")
                                                    .agg(sum("product_price").alias("total"),
                                                            collect_list(callUDF("productPriceConcat",col("product_name"),col("product_price"))).alias("products"),
                                                            max("product_price").alias("max product price"));
        customerPurchaseDf.show(false);

        //finalDf.show();
    }


}

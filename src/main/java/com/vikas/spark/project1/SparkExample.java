package com.vikas.spark.project1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class SparkExample {

    private static final String CSV_TO_DB = "CSV to DB";
    private static final String LOCAL = "local";
    private static final String CSV_RESOURCE = "src/main/resources/name_and_comments.txt";
    private static final String FORMAT = "csv";
    private static final String JDBC_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";
    private static final String USERNAME = "HR";
    private static final String PASSWORD = "test";

    public static void main(String[] args) {
//        Create a spark session
        SparkSession session = getSparkSession();

//        Get data
        Dataset<Row> dataFrame = readData(session, FORMAT, CSV_RESOURCE);

//        Transformation
        Dataset<Row> transformedDataFrame = dataFrame.withColumn("FULL_NAME",
                concat(dataFrame.col("LAST_NAME"),
                        lit(", "), dataFrame.col("FIRST_NAME"))).
                filter(dataFrame.col("COMMENTS").rlike("\\d+")).
                orderBy(dataFrame.col("LAST_NAME").asc());

//        Display top 3 rows of data
        dataFrame.show(3);

//        Save data to database
        saveToDatabase(transformedDataFrame);
    }

    private static Dataset<Row> readData(SparkSession session, String format, String csvResource) {
        return session.read().format(format).option("header", true).load(csvResource);
    }

    private static SparkSession getSparkSession() {
        return new SparkSession.Builder().appName(CSV_TO_DB).master(LOCAL).getOrCreate();
    }

    private static void saveToDatabase(Dataset<Row> dataFrame) {

        Properties properties = new Properties();
        properties.setProperty("driver", JDBC_DRIVER_CLASS);
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);
        dataFrame.write().mode(SaveMode.Overwrite).jdbc(DB_URL, "SPARK_PROJECT1", properties);
    }
}

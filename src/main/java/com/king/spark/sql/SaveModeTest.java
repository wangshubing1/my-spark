package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * SaveModel示例
 *
 * @author king
 */
public class SaveModeTest {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SaveModeTest")
                .getOrCreate();

        Dataset peopleDF = spark.read().format("json")
                .load("hdfs://spark1:9000/people.json");

        peopleDF
                .write()
                .mode(SaveMode.Append)
                .json("hdfs://spark1:9000/people_savemode_test");


    }

}

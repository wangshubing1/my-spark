package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 通用的load和save操作
 *
 * @author king
 */
public class GenericLoadSave {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("GenericLoadSave")
                .getOrCreate();

        Dataset usersDF = spark.read().load(
                "hdfs://spark1:9000/users.parquet");
        usersDF.select("name", "favorite_color").write()
                .save("hdfs://spark1:9000/namesAndFavColors.parquet");
    }

}

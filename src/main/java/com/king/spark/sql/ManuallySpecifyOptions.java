package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 手动指定数据源类型
 *
 * @author king
 */
public class ManuallySpecifyOptions {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ManuallySpecifyOptions")
                .getOrCreate();

        Dataset peopleDF = spark.read().format("json")
                .load("hdfs://spark1:9000/people.json");
        peopleDF.select("name").write().format("parquet")
                .save("hdfs://spark1:9000/peopleName_java");
    }

}

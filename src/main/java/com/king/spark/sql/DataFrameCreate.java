package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 使用json文件创建DataFrame
 *
 * @author king
 */
public class DataFrameCreate {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("DataFrameCreate")
                .getOrCreate();

        Dataset df = spark.read().json("E:\\Git\\spark-study-scala\\file\\data\\json\\student.json");

        df.show();
    }

}

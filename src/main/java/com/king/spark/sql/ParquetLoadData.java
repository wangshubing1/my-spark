package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;


/**
 * Parquet数据源之使用编程方式加载数据
 *
 * @author king
 */
public class ParquetLoadData {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ParquetLoadData")
                .getOrCreate();

        // 读取Parquet文件中的数据，创建一个DataFrame
        Dataset usersDF = spark.read().parquet(
                "hdfs://spark1:9000/spark-study/users.parquet");

        // 将DataFrame注册为临时表，然后使用SQL查询需要的数据
        usersDF.createOrReplaceTempView("users");
        Dataset<Row> userNamesDF = spark.sql("select name from users");

        // 对查询出来的DataFrame进行transformation操作，处理数据，然后打印出来
        List<String> userNames = userNamesDF.javaRDD().map(
                row -> "Name: " + row.getString(0)).collect();

        for (String userName : userNames) {
            System.out.println(userName);
        }
    }

}

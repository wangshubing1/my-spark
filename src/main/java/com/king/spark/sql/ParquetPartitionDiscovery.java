package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet数据源之自动推断分区
 *
 * @author king
 */
public class ParquetPartitionDiscovery {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ParquetPartitionDiscovery")
                .getOrCreate();

        Dataset usersDF = spark.read().parquet(
                "hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
        usersDF.printSchema();
        usersDF.show();
    }

}

package com.king.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hive数据源
 *
 * @author king
 */
public class HiveDataSource {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        // 首先还是创建SparkConf
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("HiveDataSource")
                .enableHiveSupport()
                .getOrCreate();

        // 第一个功能，使用HiveContext的sql()方法，可以执行Hive中能够执行的HiveQL语句

        // 判断是否存在student_infos表，如果存在则删除
        spark.sql("DROP TABLE IF EXISTS student_infos");
        // 判断student_infos表是否不存在，如果不存在，则创建该表
        spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
        // 将学生基本信息数据导入student_infos表
        spark.sql("LOAD DATA "
                + "LOCAL INPATH '/usr/local/spark-study/resources/student_infos.txt' "
                + "INTO TABLE student_infos");

        // 用同样的方式给student_scores导入数据
        spark.sql("DROP TABLE IF EXISTS student_scores");
        spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
        spark.sql("LOAD DATA "
                + "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
                + "INTO TABLE student_scores");

        // 第二个功能，执行sql还可以返回DataFrame，用于查询

        // 执行sql查询，关联两张表，查询成绩大于80分的学生
        Dataset goodStudentsDF = spark.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss ON si.name=ss.name "
                + "WHERE ss.score>=80");

        // 第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
        // 将DataFrame中的数据保存到hive表中

        // 接着将DataFrame中的数据保存到good_student_infos表中
        spark.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write().saveAsTable("good_student_infos");

        // 第四个功能，可以用table()方法，针对hive表，直接创建DataFrame

        // 然后针对good_student_infos表，直接创建DataFrame
        Row[] goodStudentRows = spark.table("good_student_infos").collect();
        for (Row goodStudentRow : goodStudentRows) {
            System.out.println(goodStudentRow);
        }

        spark.close();
    }

}

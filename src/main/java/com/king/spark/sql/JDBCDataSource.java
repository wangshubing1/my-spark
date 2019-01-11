package com.king.spark.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC数据源
 *
 * @author king
 */
public class JDBCDataSource {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JDBCDataSource")
                .getOrCreate();

        // 总结一下
        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

        // 分别将mysql中两张表的数据加载为DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://spark1:3306/testdb");
        options.put("dbtable", "student_infos");
        Dataset<Row> studentInfosDF = spark.read().format("jdbc")
                .options(options).load();

        options.put("dbtable", "student_scores");
        Dataset<Row> studentScoresDF = spark.read().format("jdbc")
                .options(options).load();

        // 将两个DataFrame转换为JavaPairRDD，执行join操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD =

                studentInfosDF.javaRDD().mapToPair(
                        row -> new Tuple2<>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.get(1)))))
                        .join(studentScoresDF.javaRDD().mapToPair(
                                row -> new Tuple2<>(String.valueOf(row.get(0)),
                                        Integer.valueOf(String.valueOf(row.get(1))))));
        // 将JavaPairRDD转换为JavaRDD<Row>
        JavaRDD<Row> studentRowsRDD = studentsRDD.map(line ->
                RowFactory.create(line._1, line._2._1, line._2._2));

        // 过滤出分数大于80分的数据
        JavaRDD<Row> filteredStudentRowsRDD = studentRowsRDD.filter(
                row -> {
                    if (row.getInt(2) > 80) {
                        return true;
                    }
                    return false;
                });

        // 转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> studentsDF = spark.createDataFrame(filteredStudentRowsRDD, structType);

        Row[] rows = studentsDF.collect();
        for (Row row : rows) {
            System.out.println(row);
        }

        // 将DataFrame中的数据保存到mysql表中
        // 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
        studentsDF.javaRDD().foreach(
                row -> {
                    String sql = "insert into good_student_infos values("
                            + "'" + String.valueOf(row.getString(0)) + "',"
                            + Integer.valueOf(String.valueOf(row.get(1))) + ","
                            + Integer.valueOf(String.valueOf(row.get(2))) + ")";

                    Class.forName("com.mysql.jdbc.Driver");

                    Connection conn = null;
                    Statement stmt = null;
                    try {
                        conn = DriverManager.getConnection(
                                "jdbc:mysql://spark1:3306/testdb", "", "");
                        stmt = conn.createStatement();
                        stmt.executeUpdate(sql);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    }
                }
        );

        spark.close();
    }

}

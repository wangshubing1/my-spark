package com.king.spark.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON数据源
 *
 * @author king
 */
public class JSONDataSource {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JSONDataSource")
                .getOrCreate();

        // 针对json文件，创建DataFrame（针对json文件创建DataFrame）
        Dataset<Row> studentScoresDF = spark.read().json(
                "E:\\Git\\spark-study-scala\\file\\data\\json\\students.json");

        // 针对学生成绩信息的DataFrame，注册临时表，查询分数大于80分的学生的姓名
        // （注册临时表，针对临时表执行sql语句）
        studentScoresDF.createOrReplaceTempView("student_scores");
        Dataset<Row> goodStudentScoresDF = spark.sql(
                "select name,score from student_scores where score>=80");
        goodStudentScoresDF.show();

        // （将DataFrame转换为rdd，执行transformation操作）
        List<String> goodStudentNames = goodStudentScoresDF.javaRDD().map(
                row -> row.getString(0)).collect();

        // 然后针对JavaRDD<String>，创建DataFrame
        // （针对包含json串的JavaRDD，创建DataFrame）
        List<String> studentInfoJSONs = new ArrayList<>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
        Dataset<String> studentInfosDF = spark.createDataset(studentInfoJSONs, Encoders.STRING());
        //Dataset<Row>  studentInfosDF = spark.read().json(studentInfoJSONsRDD);

        // 针对学生基本信息DataFrame，注册临时表，然后查询分数大于80分的学生的基本信息
        studentInfosDF.createOrReplaceTempView("student_infos");
        studentInfosDF.show();

        String sql = "select name,age from student_infos where name in (";
        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";

        System.out.println(sql);

        Dataset<Row> goodStudentInfosDF = spark.sql(sql);
        goodStudentInfosDF.show();

        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation
        // （将DataFrame转换为JavaRDD，再map为JavaPairRDD，然后进行join）
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                goodStudentScoresDF.javaRDD().mapToPair(
                        row -> new Tuple2<>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1)))))
                        .join(goodStudentInfosDF.javaRDD().mapToPair(
                                row -> new Tuple2<String, Integer>(row.getString(0),
                                        Integer.valueOf(String.valueOf(row.getLong(1))))));

        // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        // （将JavaRDD，转换为DataFrame）
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(
                tuple -> RowFactory.create(tuple._1, tuple._2._1, tuple._2._2));

        // 创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDF = spark.createDataFrame(goodStudentRowsRDD, structType);

        // 将好学生的全部信息保存到一个json文件中去
        // （将DataFrame中的数据保存到外部的json文件中去）
        goodStudentsDF.write().format("json").save("E:\\Git\\spark-study-java\\data\\json\\good-students-scala");
    }

}

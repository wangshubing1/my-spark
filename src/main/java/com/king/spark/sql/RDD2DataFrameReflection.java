package com.king.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * 使用反射的方式将RDD转换为DataFrame
 *
 * @author king
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args) {
        // 创建普通的RDD
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("RDD2DataFrameReflection")
                .getOrCreate();

        JavaRDD<String> lines = spark
                .sparkContext()
                .textFile("E:\\Git\\spark-study-scala\\file\\data\\student.txt", 1)
                .toJavaRDD();

        JavaRDD<Student> students = lines.map((Function<String, Student>) line -> {
            String[] lineSplited = line.split(",");
            Student stu = new Student();
            stu.setId(Integer.valueOf(lineSplited[0].trim()));
            stu.setName(lineSplited[1]);
            stu.setAge(Integer.valueOf(lineSplited[2].trim()));
            return stu;
        });

        // 使用反射方式，将RDD转换为DataFrame
        // 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
        // 因为Student.class本身就是反射的一个应用
        // 然后底层还得通过对Student Class进行反射，来获取其中的field
        // 这里要求，JavaBean必须实现Serializable接口，是可序列化的
        Dataset studentDF = spark.createDataFrame(students, Student.class);

        // 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
        studentDF.registerTempTable("students");

        // 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
        Dataset teenagerDF = spark.sql("select * from students where age<= 18");

        // 将查询出来的DataFrame，再次转换为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        // 将RDD中的数据，进行映射，映射为Student
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(
                row ->{
                    // row中的数据的顺序，可能是跟我们期望的是不一样的！
                    Student stu = new Student();
                    stu.setAge(row.getInt(0));
                    stu.setId(row.getInt(1));
                    stu.setName(row.getString(2));
                    return stu;
                });

        // 将数据collect回来，打印出来
        List<Student> studentList = teenagerStudentRDD.collect();
        for (Student stu : studentList) {
            System.out.println(stu);
        }
    }

}

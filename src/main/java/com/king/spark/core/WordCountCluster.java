package com.king.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 将java开发的wordcount程序部署到spark集群上运行
 *
 * @author king
 */
public class WordCountCluster {

    public static void main(String[] args) {
        // 如果要在spark集群上运行，需要修改的，只有两个地方
        // 第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
        // 第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件

        // 实际执行步骤：
        // 1、将spark.txt文件上传到hdfs上去
        // 2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
        // 3、将打包后的spark工程jar包，上传到机器上执行
        // 4、编写spark-submit脚本
        // 5、执行spark-submit脚本，提交spark应用到集群执行

        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");

        JavaRDD<String> words = lines.flatMap(
                s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word ->
                new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                (v1,v2)-> v1 + v2);
        wordCounts.foreach(
                wordCount ->
                        System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times."));


        sc.close();
    }

}

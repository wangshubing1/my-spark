package com.king.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 基于HDFS文件的实时wordcount程序
 *
 * @author king
 */
public class HDFSWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 首先，使用JavaStreamingContext的textFileStream()方法，针对HDFS目录创建输入数据流
        JavaDStream<String> lines = jssc.textFileStream("hdfs://spark1:9000/wordcount_dir");

        // 执行wordcount操作
        JavaDStream<String> words = lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator());


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                word -> new Tuple2(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2);

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}

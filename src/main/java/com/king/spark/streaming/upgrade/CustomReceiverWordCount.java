package com.king.spark.streaming.upgrade;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 实时wordcount程序
 *
 * @author king
 */
public class CustomReceiverWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<String> lines = jssc.receiverStream(new JavaCustomReceiver("localhost", 9999));

        JavaDStream<String> words = lines.flatMap(
                event -> Arrays.asList(event.split(" ")).iterator());


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (v1, v2) -> v1 + v2);
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}

package com.king.spark.streaming.upgrade;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 基于Flume Push方式的实时wordcount程序
 *
 * @author king
 */
public class FlumePushWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("FlumePushWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<SparkFlumeEvent> lines =
                FlumeUtils.createStream(jssc, "192.168.0.103", 8888);

        JavaDStream<String> words = lines.flatMap(
                event -> {
                    String line = new String(event.event().getBody().array());
                    return Arrays.asList(line.split(" ")).iterator();
                });

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

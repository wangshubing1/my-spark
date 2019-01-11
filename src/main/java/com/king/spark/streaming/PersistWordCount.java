package com.king.spark.streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * 基于持久化机制的实时wordcount程序
 *
 * @author king
 */
public class PersistWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("PersistWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark1", 9999);

        JavaDStream<String> words = lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator());


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                word -> new Tuple2(word, 1));


        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = (values, state) -> {
            Integer newSum = 0;
            if (state.isPresent()) {
                newSum = state.get();
            }
            for (Integer value : values) {
                newSum += value;
            }
            return Optional.of(newSum);
        };

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(updateFunction);

        // 每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便于后续的J2EE应用程序
        // 进行显示
        wordCounts.foreachRDD(wordCountsRDD -> {
            // 调用RDD的foreachPartition()方法
            wordCountsRDD.foreachPartition(wordCoun -> {
                // 给每个partition，获取一个连接
                Connection conn = ConnectionPool.getConnection();

                // 遍历partition中的数据，使用一个连接，插入数据库
                Tuple2<String, Integer> wordCount = null;
                while (wordCoun.hasNext()) {
                    wordCount = wordCoun.next();

                    String sql = "insert into wordcount(word,count) "
                            + "values('" + wordCount._1 + "'," + wordCount._2 + ")";

                    Statement stmt = conn.createStatement();
                    stmt.executeUpdate(sql);
                }

                // 用完以后，将连接还回去
                ConnectionPool.returnConnection(conn);

            });

        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}

package com.king.spark.streaming.myself;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: king
 * @Date: 2019-01-11
 * @Desc: TODO 从指定offset消费
 */

public class AppointOffset implements Serializable {
    private static final long serialVersionUID = 2166002731102216301L;

    /**
     * 从最新的offset处开始消费
     */
    public void execute(JavaStreamingContext jssc) {
        //topics，可放多个topic
        Collection<String> topics = Arrays.asList("str");

        //kafak配置
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("key.deserializer", IntegerDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "strGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<Integer, String>> dStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<Integer, String>Subscribe(topics, kafkaParams)
        );

        dStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Integer, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<Integer, String>> consumerRecordJavaRDD) throws Exception {
                //消费消息的信息: 所属topic、所属partition、消费消息数、开始offset、结束offset
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                for (OffsetRange offsetRange : offsetRanges) {
                    System.out.printf("------ topic: %s, partition:%s, count:%s, fromOffset:%s, untilOffset:%s ------\n", offsetRange.topic(), offsetRange.partition(), offsetRange.count(), offsetRange.fromOffset(), offsetRange.untilOffset());
                }

                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<Integer, String>>() {
                    @Override
                    public void call(ConsumerRecord<Integer, String> record) throws Exception {
                        System.out.printf("topic: %s, partition: %s, offset: %s, key: %s, value: %s \n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                });
            }
        });

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从指定的offset处开始消费
     */
    public void executeFromOffset(JavaStreamingContext jssc) {
        //kafak配置
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("key.deserializer", IntegerDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "strGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        //从指定offset处开始消费
        Map<TopicPartition, Long> fromOffsets = new HashMap();
        TopicPartition tp0 = new TopicPartition("str", 0);
        TopicPartition tp1 = new TopicPartition("str", 1);
        fromOffsets.put(tp0, 714880L);
        fromOffsets.put(tp1, 474817L);

        JavaInputDStream<ConsumerRecord<Integer, String>> dStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<Integer, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
        );

        dStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Integer, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<Integer, String>> consumerRecordJavaRDD) throws Exception {
                //消费消息的信息: 所属topic、所属partition、消费消息数、开始offset、结束offset
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                for (OffsetRange offsetRange : offsetRanges) {
                    System.out.printf("------ topic: %s, partition:%s, count:%s, fromOffset:%s, untilOffset:%s ------\n", offsetRange.topic(), offsetRange.partition(), offsetRange.count(), offsetRange.fromOffset(), offsetRange.untilOffset());
                }

                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<Integer, String>>() {
                    @Override
                    public void call(ConsumerRecord<Integer, String> record) throws Exception {
                        System.out.printf("topic: %s, partition: %s, offset: %s, key: %s, value: %s \n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                });
            }
        });

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

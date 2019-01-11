package com.king.learning.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: king
  * @Date: 2019-01-11
  * @Desc: TODO
  */

/**
  * 注意点：
  *
  * 依赖：
  *
  * 将lib目录下的hadoop开头jar包、hbase开头jar包添加至classpath
  *
  * 此外还有lib目录下的：zookeeper-3.4.6.jar、metrics-spark.core-2.2.0.jar（缺少会提示hbase RpcRetryingCaller: Call exception不断尝试重连hbase，不报错）、htrace-spark.core-3.1.0-incubating.jar、guava-12.0.1.jar
  *
  * $SPARK_HOME/lib目录下的 spark-assembly-1.6.1-hadoop2.4.0.jar
  *
  * 不同的package中可能会有相同名称的类，不要导错
  *
  * 连接集群：
  *
  * spark应用需要连接到zookeeper集群，然后借助zookeeper访问hbase。一般可以通过两种方式连接到zookeeper：
  *
  * 第一种是将hbase-site.xml文件加入classpath
  *
  * 第二种是在HBaseConfiguration实例中设置
  *
  * 如果不设置，默认连接的是localhost:2181会报错：connection refused
  *
  * 本文使用的是第二种方式。
  *
  * hbase创建表：
  *
  * 虽然可以在spark应用中创建hbase表，但是不建议这样做，最好在hbase shell中创建表，spark写或读数据
  */

/**
  * 向HBASE中写入数据
  *
  *
  * 代码有问题
  */
object WriteDataToHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WriteDataToHbase")
    val sc = new SparkContext(sparkConf)

    // 创建hbase的conf
    val conf = HBaseConfiguration.create()
    // 设置zookeeper集群的地址
    conf.set("hbase.zookeeper.quorum","bigdata-senior02.ibeifeng.com")
    conf.set("hbase.zookeeper.property.clientPort","2181")

    val tableName = "account"

    // 初始化JobConf,TableOutputFormat,
    // 注意: 上面两个类必须是
    val jobConf = new JobConf()
    //    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    // 创建RDD数据
    val inputDataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))

    val rdd = inputDataRDD.map(_.split(",")).map{arr=>
      /*
       * 一个put对象就是一行记录,在构造方法中指定主键,
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收3个参数: 列簇,列名,值
       */
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }


    rdd.saveAsHadoopDataset(jobConf)
    println("Save file successful !")


    sc.stop()

  }

}


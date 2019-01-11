package com.king.learning.spark.hbase

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: king
  * @Date: 2019-01-11
  * @Desc: TODO
  */

/*
先要创建表结构,无法在不存在的表中插入数据
create 'user','fam'
put 'user','4','name:test','age:10'
 */
object WriteToHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("WriteToHbase")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    write(sc)
    //    read(sc)

    sc.stop()
  }

  /**
    * 插入数据
    * 注意:
    *   写入数据的时候都转换成Bytes.toBytes(col_data)类型,否则会乱码
    *
    * @param sc   SparkContext
    */
  def write(sc: SparkContext): Unit = {
    val tableName = "user"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "bigdata-senior02.ibeifeng.com")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = new Job(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val inputDataRDD = sc.makeRDD(Array("1,Xiaoming,15", "2,Xiaohong,14", "3,Xiaogang,15"))
    val rdd = inputDataRDD.map(_.split(",")).map { arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    rdd.foreach(println)

  }

  /**
    * 读取数据
    */
  def read(sc: SparkContext) {

    // 表名
    val tableName = "user"

    // HBASE的配置
    val conf = HBaseConfiguration.create()
    // 设置zookeeper集群的地址,也可以在hbase-site.xml文件中导入classpath,
    // 但是建议在这里设置
    //    conf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    conf.set("hbase.zookeeper.quorum", "bigdata-senior02.ibeifeng.com")
    // 设置zookeeper连接的地址
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }


    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach { case (_, result) => {
      //获取行键,还必须是toString(),否则会抛异常
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("fam".getBytes, "name".getBytes))
      val age = Bytes.toInt(result.getValue("fam".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
    }


  }

}


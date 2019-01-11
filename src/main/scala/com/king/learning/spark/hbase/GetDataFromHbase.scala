package com.king.learning.spark.hbase


import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: king
  * @Date: 2019-01-11
  * @Desc: TODO
  */

/*
data
create 'student','info'
put 'student','1','info:name','xueqian'
put 'student','1','info:gender','female'
put 'student','1','info:age','23'
put 'student','2','info:name','weiliang'
put 'student','2','info:gender','male'
put 'student','2','info:age','24'
 */
/*
把HBASE的lib目录下的相关jar包copy到spark中,这些都是编程时需要引入的jar包,
自己随便找个位置放,$SPARK_HOME/jars/hbase/
hbase*.jar      hbase开头的所有jar包
guava-12.0.1.jar
protobuf-java-2.5.0.jar
htrace-spark.core-2.04.jar
 */
/*
执行命令
$SPARK_HOME/bin/spark-submit \
--driver-class-path $SPARK_HOME/jars/hbase/ *.jar:hbase/conf \
--class "GetDataFromHbase" \
spark01.jar
 */

object GetDataFromHbase {
  def main(args: Array[String]): Unit = {
    // 设置SparkConf,并创建SparkContext对象
    val sparkConf = new SparkConf()
      .setAppName("GetDataFromHbase")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    // 表名
    val tableName = "student"

    // HBASE的配置
    val conf = HBaseConfiguration.create()
    // 设置zookeeper集群的地址,也可以在hbase-site.xml文件中导入classpath,
    // 但是建议在这里设置
    //    conf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    conf.set("hbase.zookeeper.quorum", "node125,node126,node134")
    // 设置zookeeper连接的地址
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    // 读取数据并转化成RDD
    val stuRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf
      , classOf[TableInputFormat]
      , classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]
      , classOf[org.apache.hadoop.hbase.client.Result])


    val count = stuRDD.count()
    println("student RDD count : " + count)
    stuRDD.cache()


    // 遍历对象
    stuRDD.foreach { case (_, result) => {
      //获取行键,还必须是toString(),否则会抛异常
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()))
      val age = Bytes.toString(result.getValue("info".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Gender:" + gender + " Age:" + age)
    }
    }

    // the result :

    //    student RDD count : 2
    //    Row key:1 Name:xueqian Gender:female Age:23
    //    Row key:2 Name:weiliang Gender:male Age:24
    //
    //    Process finished with exit code 0


    admin.close()
    sc.stop()
  }
}

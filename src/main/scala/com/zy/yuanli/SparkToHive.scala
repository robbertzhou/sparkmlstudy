package com.zy.yuanli

import org.apache.commons.codec.StringDecoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date


/**
 * spark消费多个topic的数据写入不同的hive表
 */
object SparkToHive {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    val broker_list = "XXXX";
    val zk = "xxx";
    val confSpark = new SparkConf()
      .setAppName("kafka2hive")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.sql.shuffle.partitions", "512") //生成的partition根据kafka topic 分区生成，这个配置项貌似没效果
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
      .set("spark.streaming.backpressure.enabled", "true") //开启后spark自动根据系统负载选择最优消费速率
      .set("spark.shuffle.manager", "sort")
      .set("spark.locality.wait", "5ms")
    //.setMaster("local[*]")

    val kafkaMapParams = Map(
      "auto.offset.reset" -> "largest",
      "group.id" -> "kafka2dhive",
      "zookeeper.session.timeout.ms" -> "40000",
      "metadata.broker.list" -> broker_list,
      "zookeeper.connect" -> zk
    )
    val topicsSet = Set("innerBashData")
    val sc = new SparkContext(confSpark)
    val ssc = new StreamingContext(sc, Seconds(30)) //这个是重点微批处理，根据自己的机器资源，测试调整
    val sqlContext = new HiveContext(sc)
    var daily = sdf.format(new Date()).substring(0, 8)
    var dailyTableName = "bashdata" + daily;
    val schema = StructType(
      StructField("ver", StringType, true) ::
        StructField("session_id", StringType, true) ::
        StructField("host_time", StringType, true) ::
        StructField("host_name", StringType, true) ::
        StructField("src_ip", StringType, true) ::
        Nil)

    sqlContext.sql(
      s"""create table if not exists $dailyTableName(
                  a    string ,
                  b   string ,
                  c   string ,
                  d   string ,
                  e   string
                 )
                  PARTITIONED BY (hours string,min string)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                     """.stripMargin)


    ssc.start()
    ssc.awaitTermination()
  }
}

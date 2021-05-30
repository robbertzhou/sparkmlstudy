package com.zy.streamspark.chapter07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


/**
 * create 2021-05-29
 * author zhouyu
 * desc 批处理分析
 */
object BatchProcessAnalysis {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local[2]").appName("batch-process").getOrCreate()
    import spark.implicits._
    val logsDirector = "I:\\testdata\\datasets-master\\datasets-master\\NASA-weblogs\\nasa_dataset_july_1995"
    val rawLogs = spark.read.json(logsDirector)
    val prepairedLogs = rawLogs.withColumn("http_reply",$"http_reply".cast(IntegerType))

    val weblogs = prepairedLogs.as[WebLog]
    //println("record count is:" + weblogs.count())  //数据分析之数量
    val topDailyURLs = weblogs.withColumn("dayOfMonth",dayofmonth($"timestamp"))
      .select($"request",$"dayOfMonth")
      .groupBy($"dayOfMonth",$"request")
      .agg(count($"request").alias("count"))
      .orderBy(desc("count"))
    topDailyURLs.show(false)
  }
}

package com.zy.buildml

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object ExportHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("SparkKuduApp").getOrCreate()
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.0.126:3306")
      .option("dbtable", "enron.message")
      .option("user", "root")
      .option("password", "mima")
      .load()
    jdbcDF.printSchema()
//    val KUDU_MASTERS = "hadoop000"
//
//    // 将数据过滤后写入Kudu
    jdbcDF
      .write
      .mode(SaveMode.Append) // 只支持Append模式 键相同的会自动覆盖
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", "master.zy.com:7051")
      .option("kudu.table", "impala::default.message")
      .save()

  }

}

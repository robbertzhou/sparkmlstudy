package com.zy.buildml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * create 2020-05-05
 * author zhouyu
 */
object SparkMysqlToHive {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hdfs")
    System.load("J:/bigdata/hadoop-2.7.7/bin/hadoop.dll")
    val conf = new SparkConf()
    conf.setMaster("local[2]")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport().getOrCreate()
    // 读取MySQL数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.0.126:3306")
      .option("dbtable", "enron.message")
      .option("user", "root")
      .option("password", "mima")
      .load()
jdbcDF.show()
    jdbcDF.createOrReplaceTempView("TmpData")
    spark.sqlContext.sql("insert overwrite table  test.message select * from TmpData")
//    jdbcDF.write.mode("append").insertInto("test.message")
//    jdbcDF.write.saveAsTable("test.message")
//    jdbcDF.write.mode("append")
//      .saveAsTable("test.message")
  }
}

package com.zy.yuanli

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ff").master("local[2]").enableHiveSupport().getOrCreate()
    spark.sql("select * from tv1").show(20,false)
  }
}

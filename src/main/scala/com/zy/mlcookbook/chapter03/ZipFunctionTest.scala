package com.zy.mlcookbook.chapter03

import org.apache.spark.sql.SparkSession

/**
 * create 2021-02-13
 * author zhouyu
 * desc
 */
object ZipFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ziptest").getOrCreate()
    val signalNoise = spark.sparkContext.parallelize(Array(1,2,3))
    val signalData = spark.sparkContext.parallelize(Array(4,5,6))
    signalNoise.zip(signalData).foreach(println)
  }
}

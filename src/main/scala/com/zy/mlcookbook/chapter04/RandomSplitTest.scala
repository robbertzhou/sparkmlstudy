package com.zy.mlcookbook.chapter04

import org.apache.spark.sql.SparkSession

/**
 * @create 2021-02-15
 * @author zhouyu
 */
object RandomSplitTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("random split").getOrCreate()
    val data = spark.read.csv("i:\\testdata\\NewsAggregatorDataset\\newsCorpora.csv")
    println(data.count())
    val splitData = data.randomSplit(Array(0.8,0.2))
    val trainData = splitData(0)
    val testData = splitData(1)
    println(trainData.count())
    println(testData.count())
    println(trainData.count() + testData.count())
    spark.stop()
  }
}

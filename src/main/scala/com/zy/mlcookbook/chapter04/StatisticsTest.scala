package com.zy.mlcookbook.chapter04

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import scala.math._

/**
 * create 2021-02-14
 * author zhouyu
 * desc spark的统计api练习
 */
object StatisticsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Summary Statistics").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq(
      Vectors.dense(0,1,0),
      Vectors.dense(1,10,100),
      Vectors.dense(3,30,300),
        Vectors.dense(5,50,500),
      Vectors.dense(7,70,700),
      Vectors.dense(9,90,900),
      Vectors.dense(11,110,1100)
    ))
    val summary = Statistics.colStats(rdd)
    println("mean:" + summary.mean)
    println("variance:" + summary.variance)
    println("nonZeroNumber:" + summary.numNonzeros)
    println("max:" + summary.max)
    println("min:" + summary.min)
    println("count:" + summary.count)
    spark.stop()
    val arr = Array(0,1,3,5,7,9,11)
    var mean = 0.0
    var sum = 0.0
    for (i <- 0 to arr.length - 1){
      sum += arr(i)
    }
    mean = sum / arr.length
    var ss = 0.0
    for (i <- 0 to arr.length - 1){
      ss += pow((arr(i) - mean),2)
    }
    println(ss / (arr.length - 1))
  }
}

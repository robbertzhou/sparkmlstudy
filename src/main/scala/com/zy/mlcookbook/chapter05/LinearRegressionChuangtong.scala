package com.zy.mlcookbook.chapter05

import com.zy.common.SparkCommon

/**
 * create 2021-02-15
 * author zhouyu
 * desc 传统的线性回归
 */
object LinearRegressionChuangtong {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    val sc = spark.sparkContext
    val x = Array(1.0,5.0,8.0,10.0,15.0,21.0,27.0,30.0,38.0,45.0,50.0,64.0)
    val y = Array(5.0,1.0,4.0,11.0,25.0,18.0,33.0,20.0,30.0,43.0,55.0,57.0)
    val xRdd = sc.parallelize(x)
    val yRdd = sc.parallelize(y)
    val zipedRdd = xRdd.zip(yRdd)
    val xSum = zipedRdd.map(_._1).sum()
    val ySum = zipedRdd.map(_._2).sum()
    val xySum = zipedRdd.map(c => c._1 * c._2).sum()
    println("xsum ySum xySum",xSum,ySum,xySum)

    val n = zipedRdd.count()
    val xMean = zipedRdd.map(_._1).mean()
    val yMean = zipedRdd.map(_._2).mean()
    val xyMean = zipedRdd.map(c=>c._1 * c._2).mean()
    println("xMean,yMean,xyMean",xMean,yMean,xyMean)

    val xSquareMean = zipedRdd.map(_._1).map(x => x*x).mean()
    val ySquareMean = zipedRdd.map(_._2).map(x=>x*x).mean()
    println("xSquaremean ySquareMean",xSquareMean,ySquareMean)

    val numerator = xMean * yMean - xyMean
    val denominator = xMean * xMean - xSquareMean
    val slope = numerator / denominator
    println("slope %f5".format(slope))

    val b_intercept = yMean - (slope * xMean)
    println("bIntercept",b_intercept)

  }
}

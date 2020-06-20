package com.zy.sparkquanwei.chapter12

import com.zy.common.SparkCommon

/**
 * create 2020-06-14
 * author zy
 * desc 创建RDD
 */
object CreateRddPractice {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    //从本地集合中创建RDD，parallelize，从单个节点的数据集合中创建
    val words = "my work is local shanghai pudong load".split(" ")
    val result = spark.sparkContext.parallelize(words,2)
    result.foreach(println)
  }
}

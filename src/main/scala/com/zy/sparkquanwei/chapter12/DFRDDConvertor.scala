package com.zy.sparkquanwei.chapter12

import com.zy.common.SparkCommon

/**
 * create 2020-06-14
 * author zy
 * desc df,ds,rdd之间转换操作
 */
object DFRDDConvertor {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    //将Dataset[Long]类型转换为RDD[Long]类型
    val result = spark.range(50).rdd
    val df = spark.range(10).toDF()
    result.foreach(row =>{
      println(row)
    })
  }
}

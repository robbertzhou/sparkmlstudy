package com.zy.sparkquanwei.chapter06

import com.zy.common.SparkCommon

/**
 * create 2020-05-31
 * author zhouyu
 * desc
 */
object ProcessNumberType {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("file:///I:\\testdata\\sparkquanwei_data\\retail-data\\by-day\\2010-12-01.csv")
    df.selectExpr("CustomerId","POWER((Quantity * UnitPrice) + 5) as realQuantity").show()
  }
}

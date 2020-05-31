package com.zy.sparkquanwei.chapter06

import com.zy.common.SparkCommon
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
/**
 * create 2020-05-31
 * author zhouyu
 * desc
 */
object BooleanType {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("file:///I:\\testdata\\sparkquanwei_data\\retail-data\\by-day\\2010-12-01.csv")
    df.printSchema()
//    df.select(lit(5),lit("five"),lit(5.0)).show()
    df.where(col("InvoiceNo").equalTo(536365)).select("InvoiceNo","Description").show(5,false)

  }
}

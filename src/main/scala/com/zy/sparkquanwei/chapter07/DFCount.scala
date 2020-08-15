package com.zy.sparkquanwei.chapter07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

/**
 * create 2020-06-06
 * author zhouyu
 * desc
 */
object DFCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/data/retail_data/*.csv")
      .coalesce(5)  //重新分区,减少分区数量，因为知道这里有很多小文件
        df.cache()
    df.createOrReplaceTempView("dfTable")
//    println("number is:" + df.count())
    //创建一个日期的列
//    val dfWithDate = df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
//    dfWithDate.show()

  }
}

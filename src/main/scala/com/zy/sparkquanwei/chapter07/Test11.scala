package com.zy.sparkquanwei.chapter07

import org.apache.spark.sql.SparkSession

object Test11 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    spark.sql("select 33 as col1").show()
  }
}

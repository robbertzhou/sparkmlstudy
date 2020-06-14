package com.zy.sparkquanwei.chapter08

import com.zy.common.SparkCommon
import org.apache.spark.sql.SparkSession

/**
 * create 2020-06-08
 * author zhouyu
 * desc
 */
object InnerJoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
//    val spark = SparkCommon.getSession()
    Common.createDataframe(spark)
    val joinExr = spark.sql("select * from person").col("graduate_program") == spark.sql("select * from graduateProgram").col("id")
    println(joinExr)
  }
}

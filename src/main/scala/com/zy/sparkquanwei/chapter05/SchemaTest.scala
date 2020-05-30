package com.zy.sparkquanwei.chapter05


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.lit

/**
 * create 2020-05-30
 * author zhouyu
 * desc 模式
 */
object SchemaTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("schema").getOrCreate()
    spark.conf.set("spark.sql.caseSensitive","true")     //默认spark sql不区分大小写
    //json读取器
//    spark.read.json("file:///I:\\testdata\\sparkquanwei_data\\flight-data\\json\\2015-summary.json").show()
    //自定义schema方式读取
    val mySchemal = StructType(Array(
          StructField("DEST_COUNTRY_NAME",StringType,true),
            StructField("ORIGIN_COUNTRY_NAME",StringType,true),
            StructField("count",LongType,false)
        ))
    val df = spark.read.format("json").schema(mySchemal).load("file:///I:\\\\testdata\\\\sparkquanwei_data\\\\flight-data\\\\json\\\\2015-summary.json")
    //select operator
//    df.select(df.col("count") + 10).show()
    //select expr:创建dataframe
    df.selectExpr("*"  , //包含所有的列
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry"
    ).withColumn("onenumber",lit(1)).show()
  }
}

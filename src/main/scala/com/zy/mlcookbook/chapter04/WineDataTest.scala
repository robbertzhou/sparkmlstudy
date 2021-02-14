package com.zy.mlcookbook.chapter04

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
/**
 * create 2021-02-14
 * author zhouyu
 * desc
 */
object WineDataTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("wine data test").getOrCreate()
    import spark.implicits._
    val data = spark.read.text("I:\\testdata\\wine.data").as[String].map(parseWine)

    val df = data.toDF("id","features")
    df.show(false)

    val scale = new MinMaxScaler().setInputCol("features").setOutputCol("scaled").setMax(1).setMin(-1)
    scale.fit(df).transform(df).select("scaled").show(false)
    spark.stop()
  }

  def parseWine(str:String):(Int,Vector) ={
    val columns = str.split(",")
    (columns(0).toInt,Vectors.dense(columns(1).toFloat,columns(2).toFloat,columns(3).toFloat))
  }
}

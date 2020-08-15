package com.zy.clickhouse

import java.sql.{DriverManager, SQLFeatureNotSupportedException}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkJDBCToClickhouse {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf()
      .set("spark.debug.maxToStringFields","200")
        .setAppName("SparkJDBCToClickhouse")
        .setMaster("local[6]")

    val spark: SparkSession =
      SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()
    val filePath = "I:\\testdata\\clickhouse\\hits_v1.tsv\\hits_v1.tsv"
    // 读取people.csv测试文件内容
   val src = spark.sparkContext.parallelize(Seq((111,"上海"),(112,"中国")))
   val df = spark.createDataFrame(src).toDF("id","name")
    val ckDriver = "com.github.housepower.jdbc.ClickHouseDriver"
    val ckUrl = "jdbc:clickhouse://m.zy.com:8123/default"
    val table = "student"


    try {
      val pro = new java.util.Properties
      pro.put("driver", ckDriver)
      pro.put("user", "default")
      pro.put("password", "123456")
      df.persist().write
        .mode(SaveMode.Append)
        .option("batchsize", "2")
        .option("isolationLevel", "NONE")
        .option("numPartitions", "1")
        .jdbc(ckUrl, table, pro)
      df.unpersist()
    } catch {
      // 这里注意下，spark里面JDBC datasource用到的一些获取元数据的方法插件里并没有支持，比如getPrecision & setQueryTimeout等等，都会抛出异常，但是并不影响写入
      case e: SQLFeatureNotSupportedException =>{
        e.printStackTrace()
        println("catch and ignore!")
      }

    }
    spark.close()
  }
}

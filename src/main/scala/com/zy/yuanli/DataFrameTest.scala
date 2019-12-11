package com.zy.yuanli

import com.zy.common.SparkCommon
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val session = SparkCommon.getSession()
    val path = "file:///G:\\训练数据\\customers.txt"
    val custDF = session.read.option("header",true).format("csv").load(path)
    val maped = custDF.select(custDF("name").cast("String"),custDF("age").cast("Double"),custDF("gender").cast("String"))
    maped.printSchema()
    maped.createOrReplaceTempView("customers")
    //查询30 到35的客户
    session.sql("select * from customers where age between 20 and 35").show()
    //查询男士的客户
    session.sql("select * from customers where gender='M'").show()
  }
}

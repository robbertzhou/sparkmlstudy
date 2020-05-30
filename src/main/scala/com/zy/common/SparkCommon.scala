package com.zy.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCommon {
  def getSession(): SparkSession ={
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("app")
    val session = SparkSession.builder().config(conf).getOrCreate()
    SparkSession.setDefaultSession(session)
    session
  }
}

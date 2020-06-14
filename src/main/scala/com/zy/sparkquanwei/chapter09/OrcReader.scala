package com.zy.sparkquanwei.chapter09

import com.zy.common.SparkCommon

/**
 * create 2020-06-09
 * author zhouyu
 * desc
 */
object OrcReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
//    spark.read.jdbc("","trable",Array("predicate"),"")
    spark.read.format("orc").load("file:///I:\\testdata\\news_info\\d7fdd6b887b6fd04e4297e7190972ccd\\f\\07543ddd077247e2a9e5b28b05db9464").show()
  }
}

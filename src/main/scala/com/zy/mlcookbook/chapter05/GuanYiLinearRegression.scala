package com.zy.mlcookbook.chapter05

import com.zy.common.SparkCommon
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * create 2021-02-16
 * author zhouyu
 * desc 广义线性回归
 */
object GuanYiLinearRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    import spark.implicits._
    val data = spark.read.textFile("I:\\test_data\\spark\\housing.data").as[String]
    val regressionData = data.map{line =>
      val cols = line.replace(" ","").split(",")
      LabeledPoint(cols(13).toDouble,Vectors.dense(cols(0).toDouble,cols(1).toDouble,cols(2).toDouble,cols(3).toDouble,
        cols(4).toDouble,cols(5).toDouble,cols(6).toDouble,cols(7).toDouble))
    }
    regressionData.show(false)
    val glr = new GeneralizedLinearRegression()
        .setMaxIter(1000)
        .setRegParam(0.03)
        .setFamily("gaussian")
        .setLink("identity")

    val glrModel = glr.fit(regressionData)
    val summary = glrModel.summary

    spark.stop()
  }
}

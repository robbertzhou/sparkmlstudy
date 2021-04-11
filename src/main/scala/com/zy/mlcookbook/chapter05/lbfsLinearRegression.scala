package com.zy.mlcookbook.chapter05

import com.zy.common.SparkCommon
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * create 2021-02-16
 * author zhouyu
 */
object lbfsLinearRegression {
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
    val lr = new LinearRegression()
      .setMaxIter(1000)
      .setSolver("l-bfgs")
    val model = lr.fit(regressionData)
    model.summary
    spark.stop()
    
  }
}

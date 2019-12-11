package com.zy.yuanli

import com.zy.common.SparkCommon
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors

object PipeLine {
  def main(args: Array[String]): Unit = {
    val session = SparkCommon.getSession()
    val trainning = session.createDataFrame(Seq(
      (1.0,Vectors.dense(0.0,1.1,0.1)),
      (0.0,Vectors.dense(2.0,1.0,-1.0)),
      (0.0,Vectors.dense(2.0,1.3,1.0)),
      (1.0,Vectors.dense(0.0,1.2,-0.5))
    )).toDF("label","features")
    trainning.show()
    //create lr model
    val lr = new LogisticRegression()
    lr.setMaxIter(10).setRegParam(0.01)
    //training model
    val model = lr.fit(trainning)

    //回归预测
    //    model.transform()
    val test = session.createDataFrame(Seq(
      (1.0,Vectors.dense(0.0,1.1,0.1)),
      (0.0,Vectors.dense(2.0,1.0,-1.0)),
      (0.0,Vectors.dense(2.0,1.3,1.0)),
      (1.0,Vectors.dense(0.0,1.2,-0.5))
    )).toDF("label","features")
    val predicate = model.transform(test)
    predicate.show()
  }
}

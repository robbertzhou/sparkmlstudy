package com.zy.futureprocess

import com.zy.common.SparkCommon
import org.apache.spark.ml.feature.Binarizer

/**
  * 二值化处理，一般是针对一个特征。
  * 大于阈值为1，否则为0
  */
object LianValueHua {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    val array = Array((0,0.1),(1,0.8),(2,0.2))
    val data = spark.createDataFrame(array).toDF("id","features")
    //setThreshold  阈值
    val binarizer = new Binarizer().setInputCol("features").setOutputCol("binarizer").setThreshold(.5)
    val binaryData = binarizer.transform(data)
    binaryData.show()
  }
}

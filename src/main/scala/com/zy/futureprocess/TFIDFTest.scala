package com.zy.futureprocess

import com.zy.common.SparkCommon
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object TFIDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    //句子数据集合
    val sentenceData = spark.createDataFrame(Seq(
      (0,"Hi I heard about Spark"),
      (0,"I wish Java could use case classes"),
      (1,"Logistic regression models are neat")
    )).toDF("label","sentence")
    //列转换,拆分为单词
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordsData = tokenizer.transform(sentenceData)
    val hashHF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featuiredData = hashHF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featuiredData)
    val rescaledData = idfModel.transform(featuiredData)
    rescaledData.select("features","label").take(30).foreach(println)
  }
}

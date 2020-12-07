package com.zy.scalabigdataanalysis.chapter11

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
 * create 2020-12-07
 * author zhouyu
 *
 */
object CountVectorizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("countvectorizer").getOrCreate()
    val df = spark.createDataFrame(Seq((0,Array("Jason","David")),(1,Array("David","Martin")),
      (2,Array("Martin","Jason")),(3,Array("dd","Jason")),(4,Array("dd","Jason"))
    )).toDF("id","name")
    val cvModel : CountVectorizerModel = new CountVectorizer().setInputCol("name").setOutputCol("features")
      .setVocabSize(4).setMinDF(2).fit(df)
    cvModel.transform(df).show(false)
  }
}

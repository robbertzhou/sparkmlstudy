package com.zy.futureprocess

import com.zy.common.SparkCommon
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
 * create 2020-05-10
 * author zy
 * desc
 */
object Word2VecTest {
  def main(args: Array[String]): Unit = {
    SparkCommon.getSession()
    val spark = SparkSession.getDefaultSession.get
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about spark".split(" "),
      "I wish Java could use case classes".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    documentDF.show()
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }
}

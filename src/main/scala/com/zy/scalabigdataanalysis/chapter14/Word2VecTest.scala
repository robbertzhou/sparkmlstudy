package com.zy.scalabigdataanalysis.chapter14

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Word2Vec}
import org.apache.spark.sql.SparkSession

object Word2VecTest {
  def main(args: Array[String]): Unit = {
    val word2Vec = new Word2Vec().setInputCol("stopcols").setOutputCol("wordvector").setVectorSize(3).setMinCount(2)
    val spark = SparkSession.builder().master("local[2]").appName("textAnalysis").getOrCreate()
    val datas = Seq(
      (1,"Hello there,how do you like the book so far?"),
      (2,"I am new to Machine Learning"),
      (3,"Maybe i should get some coffee before starting"),
      (4,"Coffee is best when you driknk it hot"),
      (5,"Book stores have coffee too so i should to to a book store")
    )
    val sentenceDF = spark.createDataFrame(datas).toDF("id","sentence")
    //    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    //    val outDf = tokenizer.transform(sentenceDF)
    //    outDf.show(false)

    val regexToken = new RegexTokenizer().setInputCol("sentence").setOutputCol("regexWords").setPattern("\\W")
    val outDf = regexToken.transform(sentenceDF)
    val stopWordsRemover = new StopWordsRemover().setInputCol("regexWords").setOutputCol("stopcols")
    val stopDF = stopWordsRemover.transform(outDf)
    val word2VecModel = word2Vec.fit(stopDF)
    val word2VecDF = word2VecModel.transform(stopDF)
    word2VecDF.show(false)
  }
}

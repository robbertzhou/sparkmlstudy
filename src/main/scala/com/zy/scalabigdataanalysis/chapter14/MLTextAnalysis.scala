package com.zy.scalabigdataanalysis.chapter14

import org.apache.spark.ml.feature.{NGram, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
 * create 2020-12-12
 * author zy
 */
object MLTextAnalysis {
  def main(args: Array[String]): Unit = {
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
    val ngram = new NGram().setN(2).setInputCol("stopcols").setOutputCol("ngrams")
    ngram.transform(stopDF).show(false)
  }
}

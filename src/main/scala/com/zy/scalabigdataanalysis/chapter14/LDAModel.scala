package com.zy.scalabigdataanalysis.chapter14

import org.apache.spark.ml.clustering.LDA

/**
 * create 2020-12-17
 * author zy
 *
 */
object LDAModel {
  def main(args: Array[String]): Unit = {
    val lda = new LDA().setK(10).setMaxIter(10)
  }
}

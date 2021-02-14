package com.zy.mlcookbook.chapter02

import org.apache.spark.ml.linalg.Vectors
import breeze.linalg.{DenseVector => BreezeVector}
import org.apache.spark.ml.linalg._

/**
 * create 2021-02-11
 * author zhouyu
 * desc vector进行运算,spark接口支持的Vector可转Beeze的Vector
 */
object VectorOperation {
  def main(args: Array[String]): Unit = {
    val w1 = Vectors.dense(1,2,3)
    val w2 = Vectors.dense(4,-5,6)
    val w3 = new BreezeVector(w1.toArray)
    val w4 = new BreezeVector(w2.toArray)
    println(w3 + w4)
    println(w3 - w4)
    println(w3 * w4)
    println(w3.dot(w4))
  }
}

package com.zy.mlcookbook.chapter02

import org.apache.spark.ml.linalg.{DenseMatrix, Matrices}

/**
 * create 2021-02-11
 * author zhouyu
 * desc spark的matrix操作
 */
object SparkMatrixOp {
  def main(args: Array[String]): Unit = {
    val sparseMat33 = Matrices.sparse(3,3,Array(0,2,3,6),Array(0,2,1,0,1,2),Array(1.0,2.0,3.0,4.0,5.0,6.0))
    println(sparseMat33)
    println(sparseMat33.toArray.foreach(println))

  }
}

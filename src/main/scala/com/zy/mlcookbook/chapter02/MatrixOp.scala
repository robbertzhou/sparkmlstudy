package com.zy.mlcookbook.chapter02


import java.util.Random

import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.ml.linalg.DenseMatrix
/**
 * @create 2021-02-11
 * @author zy
 */
object MatrixOp {
  def main(args: Array[String]): Unit = {
    val data = Array(1.0,2.0,3.0,4.0)
    val dm = Matrices.dense(2,2,data)
    val vec = Vectors.dense(1.0,2.0,3.0,4.0)
    println(dm)
    println(DenseMatrix.diag(vec))
    println(DenseMatrix.eye(3))
    println("抽样均匀分布：")
    println(DenseMatrix.rand(3,3,new Random()))
    println("转换sparse")
    println(DenseMatrix.eye(3).toSparse)
  }
}

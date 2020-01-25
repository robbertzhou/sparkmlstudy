package com.zy.lineardaishu

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
/**
  * @create 2020-01-25
  * @author zhouyu
  * @desc 矩阵操作练习
  */
object MatrixOperator {
  def main(args: Array[String]): Unit = {
    val a = DenseMatrix((1,2),(3,4))
    //以列顺序排列
    val b = Matrices.dense(3,2,Array(1,3,4,5,6,9))
    println(b)
    println("a : n\n" + a)
    val m = DenseMatrix.zeros[Int](5,5)
    println("m.rows:" + m.rows + " m.cols:" + m.cols)
    //矩阵指定列
    val n = a(::,0)
    print(n)
  }
}

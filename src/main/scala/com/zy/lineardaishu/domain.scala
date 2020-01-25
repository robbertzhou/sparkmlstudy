package com.zy.lineardaishu

import breeze.linalg.DenseVector
import breeze.math.Complex
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
/**
  * @create 2020-01-25
  * @author zhouyu
  * @desc 域
  */
object   domain {
  def main(args: Array[String]): Unit = {
     // 创建dense vector
    val dv: linalg.Vector = Vectors.dense(1.0, 0.0, 3.0)
    // 创建sparse vector
    val sv1: linalg.Vector = Vectors.sparse(3, Array(0,2), Array(1.0,3.0))
    val sv2: linalg.Vector = Vectors.sparse(3, Seq((0, 1.0), (2,3.0)))

  }
}

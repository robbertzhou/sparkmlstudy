package com.zy.mlcookbook.chapter02

import org.apache.spark.ml.linalg.{SparseVector, Vectors}

/**
 * @create 2021-02-10
 * @author zy
 */
object DenseVetorSample {
  def main(args: Array[String]): Unit = {
    val customerFeatures1 :Array[Double] = Array(1,3,5,7,10,2,4,5)
    val customerFeatures2 : Array[Double] = Array(2,4,6,89,3)
    val predictedFeatures : Array[Double] = Array(0,1,0,1,0,0)
    //first create dense vector
    val x = Vectors.dense(customerFeatures1)
    val y = Vectors.dense(customerFeatures2)
    val z = Vectors.dense(predictedFeatures)

    //second create dense vector
    val denseVec2 = Vectors.dense(1,3,54,6,2)

    val zeroNums = z.toSparse.numNonzeros
    println(zeroNums)

  }
}

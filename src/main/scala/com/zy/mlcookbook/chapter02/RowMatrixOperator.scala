package com.zy.mlcookbook.chapter02

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

/**
 * create 2021-02-13
 * author zhoyyu
 * desc 行矩阵
 */
object RowMatrixOperator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("rowMatrix").getOrCreate()
    val dataVectors = Seq(
      Vectors.dense(0.0,1.0,0.0),
      Vectors.dense(3.0,1.0,5.0),
      Vectors.dense(0.0,7.0,0.0)
    )

    val identityVectors = Seq(
      Vectors.dense(1.0,0.0,0.0),
      Vectors.dense(0.0,1.0,0.0),
      Vectors.dense(0.0,0.0,1.0)
    )

    val distMatrix33 = new RowMatrix(spark.sparkContext.parallelize(dataVectors))
    println(distMatrix33)
    val statistics = distMatrix33.computeColumnSummaryStatistics()
    println(statistics.count)
    println(statistics.mean)
    println(statistics.variance)
    println(distMatrix33.computeCovariance())
    println(distMatrix33.computePrincipalComponents(2))
  }
}

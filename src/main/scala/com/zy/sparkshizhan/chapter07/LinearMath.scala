package com.zy.sparkshizhan.chapter07

import breeze.linalg.DenseVector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector,SparseVector,DenseVector}
import org.apache.spark.ml.linalg.{DenseVector => BDV,SparseVector => BSV,Vector => BV}

/**
 * create 2020-05-23
 * author zy
 * desc 线性代数
 */
object LinearMath {
  def main(args: Array[String]): Unit = {
    val dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
    val dv2 = Vectors.dense(Array(5.0,6.0,7.0,8.0))
    val sv = Vectors.sparse(4,Array(0,1,2,3),Array(5.0,6.0,7.0,8.0))
    print(dv2(2))
  }

  //vector to breezel
//  def toBreezeV(v:Vector):Vector =  {
//    case dv:BDV => new BDV(dv.values)
//    case sv:SparseVector => new BSV(sv.size,sv.indices,sv.values)
//  }
}

package com.zy.mlcookbook.chapter02

import org.apache.spark.ml.linalg.Vectors

/**
 * @create 2021-02-10
 * @author zy
 * @desc 稀疏向量练习
 */
object SparseVectorSample {
  def main(args: Array[String]): Unit = {
    val denseVec1 = Vectors.dense(5,0,3,0,0,0,0,0,0,0,8,9)
    println("vector number is:" +denseVec1.size)
    denseVec1.foreachActive((x,y)=>{
      if(y>0){
        println(x + ":" +y)
      }
    })
    val size = denseVec1.size
    val nonzeroNum = denseVec1.numNonzeros
    val arrIndex = new Array[Int](nonzeroNum)
    val arrayElement = new Array[Double](nonzeroNum)
    var indexed : Int = 0
    denseVec1.foreachActive((x,y) =>{
      if(y>0){
        arrIndex(indexed) = x
        arrayElement(indexed) = y
        indexed += 1
      }

    })
    val sparseVectors = Vectors.sparse(size,arrIndex,arrayElement)
    println(sparseVectors)
    //更简便的方法，就是DenseVector和SparseVector的互相转换(toDenseVector,toSparseVector)
    val simpleConvertor = denseVec1.toSparse
    println(simpleConvertor)
  }
}

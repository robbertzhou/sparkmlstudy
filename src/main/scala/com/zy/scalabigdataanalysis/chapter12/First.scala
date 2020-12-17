package com.zy.scalabigdataanalysis.chapter12

object First {
  def main(args: Array[String]): Unit = {
    val sampHash = "244462280684106696397174876150683825260"
    val sh = new Array[Int](sampHash.length)
    for (i <- 0 to sampHash.length - 1){
      sh(i) = sampHash(i).toInt
    }
    val urlHash = "241055486143303376188389549170312772780"
    val uh = new Array[Int](urlHash.length)
    for (i <- 0 to urlHash.length - 1){
      uh(i) = urlHash(i).toInt
    }
    val kk = hammingDistanceI(sh,uh)
    println("ewew")
  }

  def hammingDistanceI(v1:Array[Int], v2:Array[Int]) = {
    v1.zip(v2).count{case(a,b) => a!=b}
  }
}

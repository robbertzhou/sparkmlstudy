package com.zy.mlcookbook.chapter04

object ImplicitOps1 {
  def main(args: Array[String]): Unit = {
    var numInt:Int=5
    val numFloat:Float=numInt
    println(numInt,numFloat)
    val str="Scala"
    numInt=str
    println(str,numInt)
  }
  implicit def int2float(num:Int):Float=num.intValue()
  implicit def string2Int(str:String):Int=str.size
}

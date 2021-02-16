package com.zy.mlcookbook.chapter04

object ImplicitTest {
  def main(args: Array[String]): Unit = {
    val kk : Int = 11
    var ss :String = ""
    var ff : Float = 3.0f
    ff = kk
    println(ss.getClass)
//    func(11)
  }

  def func(msg:String) = println(msg)
  implicit def int2float(num:Int):Float=num.intValue()

  implicit def intToString(i:Int) = i.toString


}

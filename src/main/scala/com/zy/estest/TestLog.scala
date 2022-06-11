package com.zy.estest

object TestLog {
  def main(args: Array[String]): Unit = {
    val str = "java -cp log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain > log/test"
    for (i <- 10 to 5000){
      println(str + i + ".log")
    }
  }
}

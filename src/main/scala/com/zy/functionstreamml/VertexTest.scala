package com.zy.functionstreamml

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

case class User(name:String,occupation:String)
/**
 * create 2020-08-16
 * author zhouyu
 *
 */
object VertexTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
    val sc = spark.sparkContext
    val users = sc.textFile("user.txt").map{line =>
      val fields = line.split(",")
      (fields(0).toLong,User(fields(1),fields(2)))
    }

    val friends = sc.textFile("friends.txt").map{line =>
      val fields = line.split(",")
      Edge(fields(0).toLong,fields(1).toLong,"friend")
    }

    //build graphx
    val graphx = Graph(users,friends)
    //过滤filter函数
    graphx.vertices.filter(x => x._2.occupation=="Doctor").take(10).foreach(println)
  }
}



package com.zy.sortedtest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(SortDemo.getClass.getName)
    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.makeRDD(List("zhangsan 18 60", "lisi 21 70", "wangmazi 20 70",
      "kuangsan 26 99"))
    val file: RDD[(String, Int, Int)] = data.map({
      line => {
        val fields = line.split(" ")
        val name = fields(0)
        val age = fields(1).toInt
        val fv = fields(2).toInt
        (name, age, fv)
      }
    })

    //    file.sortBy(t => (-t._3,t._2)).foreach(println)  元组的直接排序，最简单的方法。
    file.sortBy(t => new Person(t._1,t._2,t._3)).foreach(println)

  }
}

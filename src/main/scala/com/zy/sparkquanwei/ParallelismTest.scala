package com.zy.sparkquanwei

import org.apache.spark.{SparkConf, SparkContext}

/**
 * create 2021-04-11
 * author zhouyu
 * desc 并行度练习
 */
object ParallelismTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("day365").setMaster("local")
    conf.set("spark.default.parallelism","8")  //默认并行度
    val sc = new SparkContext(conf)
    f1(sc)
  }

  def f1(sc:SparkContext): Unit ={
    val data = sc.parallelize(1 to 100,10)
    println("原始partition长度：" + data.partitions.length)
    val mapRdd = data.map(x => x)
    println("map影射后的partition长度：" + mapRdd.partitions.length)
    val repRdd = mapRdd.repartition(10)
    println("repartition后的partition长度：" + repRdd.partitions.length)
    val mapRdd2 = repRdd.map(x=>(x,1))
    println("[再map] mapRdd2.partitions.length="+mapRdd2.partitions.length)

    //如果conf设置了 spark.default.parallelism 这个属性,
    // 那么在groupByKey操作(这里的groupByKey指shuffle操作的算子)不指定参数时会默认到的读取设置的默认并行度参数
    val groupRdd = mapRdd2.groupByKey(6)
    println("[groupByKey] groupRdd.partitions.length="+groupRdd.partitions.length)
  }
}

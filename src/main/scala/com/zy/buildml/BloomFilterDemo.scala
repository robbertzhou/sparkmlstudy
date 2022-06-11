package com.zy.buildml

import org.apache.commons.httpclient.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.client.{HdfsDataInputStream, HdfsDataOutputStream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.sketch.BloomFilter

case class General(name:String,age:Int)
object BloomFilterDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BloomFilterDemo")
      .master("local[*]")
      .getOrCreate()

    val output = "D:\\aarontest\\data\\777.txt"

    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(
      General("张辽",227), General("司马懿",188), General("扎克",100)
    )).toDF


    val rdd = spark.sparkContext.parallelize(Seq("赵信","司马懿","扎克"))
//    val bl = df.stat.bloomFilter("name",20L,0.01)
//    import org.apache.hadoop.fs.FileSystem
//    // 1 获取文件系统// 1 获取文件系统
//
    val configuration = new Configuration()

    // 配置在集群上运行
    configuration.set("fs.defaultFS", "hdfs://master.zy.com:8020")
    configuration.set("dfs.replication", "2") //指定上传文件的副本数量


    val fs = FileSystem.get(configuration )
//    val outputStream = fs.create(new Path("/data/bf"))
//    bl.writeTo(outputStream)
    val inputStream = fs.open(new Path("/data/bf"))
    val bbff = BloomFilter.readFrom(inputStream)
    rdd.map(x => {
      val isExists = bbff.mightContainString(x)
      print(isExists)
    }).foreach(print)

  }
}

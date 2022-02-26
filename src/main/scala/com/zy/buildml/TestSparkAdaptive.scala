package com.zy.buildml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkAdaptive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        conf.setMaster("local[2]")
    conf.setAppName("app")
//    1、打开自适应框架的开关
//    spark.sql.adaptive.enabled true
//
//    2、设置partition的上下限
//    spark.sql.adaptive.minNumPostShufflePartitions 10
//    spark.sql.adaptive.maxNumPostShufflePartitions 2000
//
//    3、设置单reduce task处理的数据大小
//      spark.sql.adaptive.shuffle.targetPostShuffleInputSize 134217728  128M
//    spark.sql.adaptive.shuffle.targetPostShuffleRowCount 10000000
//    spark.sql.adaptive.enabled 默认为false 自适应执行框架的开关
//    spark.sql.adaptive.skewedJoin.enabled 默认为 false  倾斜处理开关
//      spark.sql.adaptive.skewedPartitionFactor 默认为10 当一个partition的size大小 大于 该值乘以所有parititon大小的中位数 且 大于spark.sql.adaptive.skewedPartitionSizeThreshold，或者parition的条数大于该值乘以所有parititon条数的中位数且 大于 spark.sql.adaptive.skewedPartitionRowCountThreshold， 才会被当做倾斜的partition进行相应的处理
//    spark.sql.adaptive.skewedPartitionSizeThreshold 默认为67108864 倾斜的partition大小不能小于该值，该值还需要参照HDFS使用的压缩算法以及存储文件类型（如ORC、Parquet等）
//    spark.sql.adaptive.skewedPartitionRowCountThreshold  默认为10000000 倾斜的partition条数不能小于该值
//    spark.shuffle.statistics.verbose 默认为false 打开后MapStatus会采集每个partition条数的信息，用于倾斜处理
    conf.set("spark.sql.adaptive.enabled","true")
    conf.set("spark.sql.adaptive.minNumPostShufflePartitions","1")
    conf.set("spark.sql.adaptive.maxNumPostShufflePartitions","2")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize","1342177280")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleRowCount","100000000")

    val session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    SparkSession.setDefaultSession(session)
    session.sql("select * from student_tb_txt").createOrReplaceTempView("tmp2")
//    session.sql("insert into table test1 select * from tmp2")  overwrite SELECT age, name FROM person DISTRIBUTE BY age;
    session.sql("insert overwrite table test_parquet select * from tmp2 DISTRIBUTE BY s_no")
  }
}

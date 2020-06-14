package com.zy.sparkquanwei.chapter09

import com.zy.common.SparkCommon
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job

object Test {
  def main(args: Array[String]): Unit = {


    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.rootdir", "hdfs://master.zy.com:8020/hbase")
    //    hconf.set(TableInputFormat.INPUT_TABLE, "demo_label_test")
    val spark = SparkCommon.getSession()
    val sc = spark.sparkContext

    //    val rdd = sc.textFile("hdfs://ns20.data.yoyi:8020/hbase/data/default/demo_label_test/fd47c44b1b341703f0ab40a1c47959f6/c")
    //    rdd.foreach(println)

    //    val hbaseContext = new HBaseContext(sc, hconf)
//    val scan = new Scan()
    //    val rdd = hbaseContext.hbaseRDD(TableName.valueOf("demo_label_test"), scan)
//    val proto = ProtobufUtil.toScan(scan)
//    hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()))
    val job = Job.getInstance(hconf)
    val snapName = "first"
    val path = new Path("hdfs://master.zy.com:8020/data/hbase/first")
    TableSnapshotInputFormat.setInput(job, snapName, path)
    val rdd = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.foreach(t => {
      val result = t._2
      val rowkey = new String(result.getRow)
      println(rowkey)
      val cells = result.rawCells()
      for (cell <- cells) {
        val qualifierArray = cell.getQualifierArray
        val valueArray = cell.getValueArray
        val key = new String(qualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value = new String(valueArray, cell.getValueOffset, cell.getValueLength)
        println("key: " + key)
        println("value: " + value)
      }

    })
    //        println(rdd.count())

    sc.stop()
  }
}

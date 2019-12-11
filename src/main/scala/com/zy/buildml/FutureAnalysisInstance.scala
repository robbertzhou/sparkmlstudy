package com.zy.buildml

import com.zy.common.SparkCommon
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object FutureAnalysisInstance {
  def main(args: Array[String]): Unit = {
    val session = SparkCommon.getSession()
    val path = "file:///G:\\训练数据\\电影数据\\ml-1m\\users.dat"
    val text = session.sparkContext.textFile(path)  //extract data
    import session.sqlContext._
    //transformation
    val maped = text.map(line=>{
      val splits = line.toString().split("::")
      Row(splits(0),splits(1),splits(2),splits(3),splits(4))
    })
    //构造dataframe【数据结构化】
    val st = new StructType(
      Array(
        StructField("userid",DataTypes.StringType,true),
        StructField("gender",DataTypes.StringType,true),
        StructField("age",DataTypes.StringType,true),
        StructField("occupation",DataTypes.StringType,true),
        StructField("zip",DataTypes.StringType,true)
      )
    )
    val df = session.createDataFrame(maped,st)
    df.createTempView("users")
    val age = session.sql("select age from users")
    age.show(300)
  }
}

package com.zy.sparkquanwei.chapter08

import org.apache.spark.sql.SparkSession

/**
 * create 2020-06-08
 * author zhouyu
 * desc
 */
object Common {
    def createDataframe(spark:SparkSession): Unit ={
      val person = Seq(
        (0,"Bill Chambers",0,Seq(1000)),
        (1,"Matei Zaharia",1,Seq(500,250,100)),
        (2,"Michael Armbrust",2,Seq(350,100))
      )
      spark.createDataFrame(person).toDF("id","name","graduate_program","spark_status")
        .createOrReplaceTempView("person")
      val graduateProgram = Seq(
        (0,"Masters","School of Information","UC Berkeley"),
        (2,"Masters","EECS","UC Berkeley"),
        (1,"Ph.D.","EECS","UC Berkeley")
      )
      spark.createDataFrame(graduateProgram).toDF("id","degree","department","school").createOrReplaceTempView("graduateProgram")
      val sparkStatus = Seq(
        (500,"Vice President"),
        (250,"PMC Member"),
        (100,"Contributor"))
      spark.createDataFrame(sparkStatus).toDF("id","status").createOrReplaceTempView("sparkStatus")
    }
}

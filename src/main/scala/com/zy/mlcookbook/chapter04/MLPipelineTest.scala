package com.zy.mlcookbook.chapter04

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
 * create 2021-02-14
 * author zhouyu
 * desc ml管道练习
 */
object MLPipelineTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ml pipeline").master("local[2]").getOrCreate()
    val trainset = spark.createDataFrame(Seq(
      (1L,1,"spark rocks"),
      (2L,0,"flink is the best"),
      (3L,1,"Spark rules"),
      (4L,0,"Kafka is great"),
      (5L,0,"mapreduce forever")
    )).toDF("id","label","words")
    val tokenizer = new Tokenizer().setInputCol("words").setOutputCol("tokens")
    val hasingTF = new HashingTF().setNumFeatures(1000).setInputCol("tokens").setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(15).setRegParam(0.01)
    val pipeline = new Pipeline().setStages(Array(tokenizer,hasingTF,lr))
    val model = pipeline.fit(trainset)

    val testSet = spark.createDataFrame(Seq(
      (10L,1,"use spark please"),
      (11L,2,"kafka")
    )).toDF("id","label","words")
    model.transform(testSet).select("probability","prediction").show(false)
    spark.stop()
  }
}

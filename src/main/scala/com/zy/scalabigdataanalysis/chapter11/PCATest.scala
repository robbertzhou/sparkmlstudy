package com.zy.scalabigdataanalysis.chapter11

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * create 2020-12-09
 * author zy
 */
object PCATest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("countvectorizer").getOrCreate()
    val data =Array(
      Vectors.dense(3.5,2.0,5.0,6.3,5.6,2.4),
      Vectors.dense(4.4,0.10,3.0,9.0,7.0,8.75),
      Vectors.dense(3.2,2.4,0.0,6.0,7.4,3.34)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(4).fit(df)
    val result = pca.transform(df)
    result.show(false)

//    val data1 =Seq(
//      (3.5,2.0,5.0),
//      (4.4,0.10,3.0),
//      (3.2,2.4,0.0)
//    )
//    val tt = spark.createDataFrame(data1).toDF("id", "negative_logit", "positive_logit")
//    val assembler = new VectorAssembler()
//
//      .setInputCols(Array("id","negative_logit", "positive_logit"))
//
//      .setOutputCol("prediction")
//
//    val output = assembler.transform(tt).toDF("prediction")
//    output.show(false)

  }
}

package com.zy.futureprocess

import org.ansj.domain.Term
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}

import org.apache.spark.mllib.linalg.{ SparseVector => SV }
import scala.collection.mutable.ArrayBuffer

/**
 * create 2020-05-10
 * author zy
 *
 */
object Xiangsi {

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("tdidf")
    val sc =new SparkContext(conf)
    //读取2600份法律案例
    val rdd = sc.wholeTextFiles("hdfs://master:8020/data2/*")
    val text =rdd.map { case (file,text) => text }
    val title =rdd.map { case (title,text) => title }.collect()
    val dim = math.pow(2,18).toInt
    val hashingTF =new HashingTF(dim)
    val filter =new StopRecognition()
    filter.insertStopNatures("w")//过滤掉标点
    //使用ansj对文本进行分词
    val tokens2 =text.map(doc => ToAnalysis.parse(doc).recognition(filter).toStringWithOutNature(" ").split(" ").toSeq)
    //tf计算
    val tf =hashingTF.transform(tokens2)
    // cache data in memory
    tf.cache
    //idf计算
    val idf =new IDF().fit(tf)

    val tfidf =idf.transform(tf)
    val vectorArr =tfidf.collect()
    //需要匹配相似度的文本
    val rdd3 = sc.parallelize(Seq("被告人王乃胜，高中文化，户籍所在地:白山市，居住地:白山市。"))
    val predictTF2 =rdd3.map(doc => hashingTF.transform(ToAnalysis.parse(doc).recognition(filter).toStringWithOutNature(" ").split(" ").toSeq))
    val predictTfIdf = idf.transform(predictTF2)
    import breeze.linalg._
    val predictSV =predictTfIdf.first.asInstanceOf[SV]
    val c =new ArrayBuffer[(String, Double)]()
    val breeze1 = new SparseVector(predictSV.indices,predictSV.values,predictSV.size)
    var tag =0
    for (i <-vectorArr) {
      val Svector =i.asInstanceOf[SV]
      val breeze2 =new SparseVector(Svector.indices,Svector.values, Svector.size)
      val cosineSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
      c ++= Array((title(tag),cosineSim))
      tag += 1
    }
    val cc =c.toArray.sortBy(_._2).reverse.take(10)
    println(cc.toBuffer)
    sc.stop()
  }


  def  splitWordToSeq(news:String)={
    val terms:java.util.List[Term] =ToAnalysis.parse(news).getTerms;
    val size=terms.size()
    var res="";
    for( i<- 0 until size){
      res+=terms.get(i.toInt).getName+" "
    }
    res.split(" ")
  }

}

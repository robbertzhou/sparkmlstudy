package com.zy.mlcookbook.chapter07

import java.awt.Font
import java.util

import com.zy.common.SparkCommon
import org.ansj.splitWord.analysis.BaseAnalysis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.data.category.DefaultCategoryDataset

import scala.collection.JavaConverters._

/**
 * create 2021-02-27
 * author zy
 */
object WordFreqStatistics {
  def main(args: Array[String]): Unit = {
    val spark = SparkCommon.getSession()
    val sc = spark.sparkContext
    val lineOfBook = sc.textFile("I:\\testdata\\textdata\\pg62.txt")
      .flatMap(line =>{
        val list = BaseAnalysis.parse(line)
        val it = list.iterator()
        val array = new util.ArrayList[String]()
        while (it.hasNext){
          val item = it.next()
          array.add(item.getName)
        }
        array.asScala
      })
      .map(word => (word,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
    val top25 = lineOfBook.take(25)
    val dataset = new DefaultCategoryDataset()
    top25.foreach({case(term:String,count:Int) =>{
      dataset.setValue(count,"Count",term)
    }})
    val chart = ChartFactory.createBarChart("Term frequency","Words","Count",
      dataset,PlotOrientation.VERTICAL,false,true,false)
    val font = new Font("SimSun", 10, 20)
    chart.getTitle.setFont(font)
    val plot = chart.getCategoryPlot()

    val domainAxis = plot.getDomainAxis()
    show(chart)
    spark.stop()
  }

  def show(chart:JFreeChart): Unit ={
    val frame = new ChartFrame("",chart)
    frame.pack()
    frame.setVisible(true)
  }
}

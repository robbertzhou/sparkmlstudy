package com.zy.mlcookbook.chapter04

import org.apache.spark.sql.SparkSession

/**
 * @create 2021-02-15
 * @author zhouyu
 * @desc dataset api练习
 */
object DatasetApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("dataset api").getOrCreate()
    import spark.implicits._
    val champs = spark.createDataset(List(Team("Broncos","Denver"),Team("Patriots","New England")))
    val data = spark.read.csv("i:\\testdata\\cars.json")
    champs.show(false)
    val teams = spark.read.option("header","true")
        .csv("i:\\testdata\\cars.json")
        .as[Team]
    teams.show(false)
    val cities = teams.map(t => t.city)
    cities.show(false)
    cities.explain()
    spark.stop()

  }
}

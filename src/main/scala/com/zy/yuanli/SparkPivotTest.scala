package com.zy.yuanli

import org.apache.spark.sql.SparkSession

object SparkPivotTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").enableHiveSupport().getOrCreate()
//    spark.sql(" SELECT      *  FROM test.scores PIVOT(     MAX(score) FOR course in ('Chinese', 'Math', 'English') )").show()
    val pivot =
      """
        |select * from
        |dw.userprofile_attribute_all
        |pivot (max(2)  for labelid in ('ACTION_U_05_03','ACTION_U_05_01','ACTION_U_05_02','ATTRIBUTE_U_06_005'))
        |""".stripMargin

//    val df = spark.sql(pivot)
//    spark.sql("create table test.tbl (id string) stored as orc")
//    val schema = df.schema
//    val fields = schema.fields
//    for (x <- fields){
//      println(x)
//    }

    val sel =
      """
        |select userid,
        |concat_ws(',',collect_set(concat(concat('"',labelid,'"'),':',labelweight))) as lw
        |from dw.userprofile_attribute_all
        |group by userid
        |""".stripMargin

    spark.sql(sel).show()

//    ACTION_U_05_03
//    ACTION_U_05_01
//    ACTION_U_05_02
//    ATTRIBUTE_U_06_005
  }

}

package com.zy.yuanli

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

object TestLauncher {
  def main(args: Array[String]): Unit = {
    val handle: SparkAppHandle = new SparkLauncher()
      .setSparkHome("/path/to/spark/home")
      .setAppResource("/path/to/your/spark/program/jar")
      .setConf("spark.driver.memory", "2")
      .setConf("spark.driver.extraClassPath", "/your/class/path")
      .setConf("spark.executor.extraClassPath", "/your/class/path")
      .setConf("spark.executor.memory", "2")
      .setConf("spark.executor.cores", "10")
      .setConf("spark.num.executors", "5")
      .setMainClass("XXXXCLASS")
      .setVerbose(true)
      .setMaster("yarn")
      .setDeployMode("cluster")
      .startApplication(new SparkAppHandle.Listener {
        override def stateChanged(handle: SparkAppHandle): Unit = {
        }
        override def infoChanged(handle: SparkAppHandle): Unit = {
        }
      })
  }
}

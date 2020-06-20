package com.zy.estest

import com.alibaba.fastjson.JSON
import com.zy.common.SparkCommon
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index, Update, UpdateByQuery}
import org.apache.spark.rdd.RDD

object MyEsUtils {
  def main(args: Array[String]): Unit = {
//    works_tort_plat_index
    val spark = SparkCommon.getSession()
    val rdd = spark.sql("select 'asfd' as id,'jack' as name").rdd
    import spark.implicits._
    val ll  = spark.sql("select '553443' as id,'sddsss' as field1").toDF().toJSON.map(str=>{
      val id = JSON.parseObject(str).get("id").toString
      (id,str)
    }).foreachPartition(ff=>{
      insertBulk("works_tort_plat_index",ff)
    })
//      .foreachPartition(ff=>{
//      insertBulk("works_tort_plat_index",ff)
//    })
//    insertBatchIntoEs(ll)

  }

  // 单挑插入
  def insertSingleIntoEs(rdd: RDD[Any])={
    rdd.foreachPartition(it=>{
      // 获取连接
      val jestClient: JestClient = MyEsUtils.getClient() // 一个分区获取一次连接
      it.foreach(movie=>{
        MyEsUtils.insertIntoEs(jestClient,"movie_info",movie) //逐行插入
      })
      jestClient.close()
    })
  }

  // 批次插入
  def insertBatchIntoEs(rdd: RDD[Any])={
//    if(rdd.count()>10000*10){
//      rdd.repartition((rdd.count()/10000).toInt)// 从新分区保证每次分区的数据最多为10000条
//    }
//    rdd.foreachPartition(it=>{
//      MyEsUtils.insertBulk("movie_info",it);
//    })
  }

  val esUrl: String ="http://192.168.0.110:9200"  // es 集群地址
  val factory: JestClientFactory = new JestClientFactory()  // 创建工厂类
  val config: HttpClientConfig = new HttpClientConfig.Builder(esUrl) // 设置配置参数
    .multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000)
    .readTimeout(10000)
    .build()
  factory.setHttpClientConfig(config) //给工厂类设置参数

  // 定义方法返回客户端
  def getClient(): JestClient ={
    factory.getObject
  }

  // 逐条写入
  def insertIntoEs(jestClient: JestClient,indexName:String,source:Any)={
    val indexAction: Index = new Index.Builder(source).index(indexName).`type`("_doc").build()
    jestClient.execute(indexAction);
  }

  // 逐条写入
  def insertIntoEs(indexName:String,source:Any)={
    val client: JestClient = factory.getObject
    val indexAction: Index = new Index.Builder(source).index(indexName).`type`("_doc").build()
    client.execute(indexAction);
    client.close()
  }

  /**
   * 批量插入 对接Rdd的分区数据
   * @param indexName
   * @param sources
   */
  def insertBulk(indexName:String,sources: Iterator[(String,String)])={
    val client: JestClient = factory.getObject
    val bulkBilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    bulkBilder.refresh(true)
    // 对数据进行匹配
    sources.foreach(
      {
        case (id:String,source)=>{ // 如果传递了Id则使用传的Id
//          val jj = JSON.parseObject("{\"id\":\"周瑜\",\"name\":\"asd\"}",classOf[Movie])
          bulkBilder.addAction(new Index.Builder(source).id(id).build())
        }
        case source=>{
          bulkBilder.addAction(new Index.Builder(source).build()) //没传Id则使用随机生成的
        }
      }
    )
    client.execute(bulkBilder.build())
    client.close()
  }
  def update(): Unit ={
    Update.Builder
  }
}



case class  Movie(id:String,name:String) // 样例类

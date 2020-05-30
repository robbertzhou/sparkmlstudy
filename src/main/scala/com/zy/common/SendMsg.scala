package com.zy.common

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.zy.entities.Student

import scala.util.Random

/**
 * create 2020-05-30
 * author zhouyu
 * desc
 */
object SendMsg {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "m.zy.com:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    //指定topic名称
    val topic = "student_topic"
    //create kafka connection
    val producer = new KafkaProducer[String,String](props)
    val random = new Random()
    while (true) {

      val jsonObject = new JSONObject()
      val stu = new Student
      stu.setId(random.nextInt(200000000))
      stu.setName("姓名," + stu.getId)
//      val detail = generateDetail(tm.id_a)
      val detailStr = JSON.toJSONString(stu,SerializerFeature.WriteMapNullValue)
      producer.send(new ProducerRecord[String,String](topic,detailStr ))
      //发送批到kafka
      Thread.sleep(1)
      println("send success")
    }
  }
}

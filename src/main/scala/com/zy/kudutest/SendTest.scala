package com.zy.kudutest

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object SendTest {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "slave1.zy.com:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    //指定topic名称
    val topic = "tt_topic"
    //create kafka connection
    val producer = new KafkaProducer[String, String](props)
    while (true) {
      val s1 = "{\"id\":3,\"name\":\"jack\",\"valll\":\"valll\"}"
      producer.send(new ProducerRecord[String, String](topic, s1 ))
      //发送批到kafka
      Thread.sleep(100)
      println("send success")
    }
  }
}

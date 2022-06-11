package com.zy;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkStreamingKafka {
    public static void main(String[] args) throws InterruptedException {
        String brokers = "slave2.zhou.com:9092";
        String topics = "mysql_topic";
        SparkConf conf = new SparkConf().setAppName("streaming word count");
//        setMaster("local[2]")
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
//        "auto.offset.reset" -> "earliest"
//        "enable.auto.commit" -> (false: java.lang.Boolean)
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区  也可以通过配置项实现
        //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
        //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        //kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit",false);

//        offsets.put(new TopicPartition("topic1", 0), 2L);
        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );
        //这里就跟之前的demo一样了，只是需要注意这边的lines里的参数本身是个ConsumerRecord对象
//        JavaPairDStream<String, Integer> counts =
//                lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
//                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
//                        .reduceByKey((x, y) -> x + y);

//  可以打印所有信息，看下ConsumerRecord的结构
      lines.foreachRDD(rdd -> {
          rdd.foreach(x -> {
//              String val = x.value().toString();
//              JSONObject obj = (JSONObject) JSON.parse(val);
//              System.out.println(obj.get("data"));
              System.out.println(x.value().toString());
          });
        });
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}

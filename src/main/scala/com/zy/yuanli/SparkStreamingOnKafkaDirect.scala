package com.zy.yuanli

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * SparkStreaming2.3版本 读取kafka 中数据 ：
 *  1.采用了新的消费者api实现，类似于1.6中SparkStreaming 读取 kafka Direct模式。并行度 一样。
 *  2.因为采用了新的消费者api实现，所有相对于1.6的Direct模式【simple api实现】 ，api使用上有很大差别。未来这种api有可能继续变化
 *  3.kafka中有两个参数：
 *      heartbeat.interval.ms：这个值代表 kafka集群与消费者之间的心跳间隔时间，kafka 集群确保消费者保持连接的心跳通信时间间隔。这个时间默认是3s.
 *             这个值必须设置的比session.timeout.ms appropriately 小，一般设置不大于 session.timeout.ms appropriately 的1/3
 *      session.timeout.ms appropriately：
 *             这个值代表消费者与kafka之间的session 会话超时时间，如果在这个时间内，kafka 没有接收到消费者的心跳【heartbeat.interval.ms 控制】，
 *             那么kafka将移除当前的消费者。这个时间默认是10s。
 *             这个时间位于配置 group.min.session.timeout.ms【6s】 和 group.max.session.timeout.ms【300s】之间的一个参数,如果SparkSteaming 批次间隔时间大于5分钟，
 *             也就是大于300s,那么就要相应的调大group.max.session.timeout.ms 这个值。
 *  4.大多数情况下，SparkStreaming读取数据使用 LocationStrategies.PreferConsistent 这种策略，这种策略会将分区均匀的分布在集群的Executor之间。
 *    如果Executor在kafka 集群中的某些节点上，可以使用 LocationStrategies.PreferBrokers 这种策略，那么当前这个Executor 中的数据会来自当前broker节点。
 *    如果节点之间的分区有明显的分布不均，可以使用 LocationStrategies.PreferFixed 这种策略,可以通过一个map 指定将topic分区分布在哪些节点中。
 *
 *  5.新的消费者api 可以将kafka 中的消息预读取到缓存区中，默认大小为64k。默认缓存区在 Executor 中，加快处理数据速度。
 *     可以通过参数 spark.streaming.kafka.consumer.cache.maxCapacity 来增大，也可以通过spark.streaming.kafka.consumer.cache.enabled 设置成false 关闭缓存机制。
 *
 *  6.关于消费者offset
 *    1).如果设置了checkpoint ,那么offset 将会存储在checkpoint中。
 *     这种有缺点: 第一，当从checkpoint中恢复数据时，有可能造成重复的消费，需要我们写代码来保证数据的输出幂等。
 *                第二，当代码逻辑改变时，无法从checkpoint中来恢复offset.
 *    2).依靠kafka 来存储消费者offset,kafka 中有一个特殊的topic 来存储消费者offset。新的消费者api中，会定期自动提交offset。这种情况有可能也不是我们想要的，
 *       因为有可能消费者自动提交了offset,但是后期SparkStreaming 没有将接收来的数据及时处理保存。这里也就是为什么会在配置中将enable.auto.commit 设置成false的原因。
 *       这种消费模式也称最多消费一次，默认sparkStreaming 拉取到数据之后就可以更新offset,无论是否消费成功。自动提交offset的频率由参数auto.commit.interval.ms 决定，默认5s。
 *       如果我们能保证完全处理完业务之后，可以后期异步的手动提交消费者offset.
 *
 *    3).自己存储offset,这样在处理逻辑时，保证数据处理的事务，如果处理数据失败，就不保存offset，处理数据成功则保存offset.这样可以做到精准的处理一次处理数据。
 *注释：这里的解释是北京尚学堂的我敬佩的大佬讲师的
 */
object SparkStreamingOnKafkaDirect {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    //    conf.setMaster("local")
    //    conf.setAppName("SparkStreamingOnKafkaDirect")
    val spark  = SparkSession.builder().appName("test").master("local[2]").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Durations.seconds(3))
    //设置日志级别
    ssc.sparkContext.setLogLevel("Error")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master.zy.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "adfwewqe",//

      /**
       * 当没有初始的offset，或者当前的offset不存在，如何处理数据
       *  earliest ：自动重置偏移量为最小偏移量
       *  latest：自动重置偏移量为最大偏移量【默认】
       *  none:没有找到以前的offset,抛出异常
       */
      "auto.offset.reset" -> "earliest",

      /**
       * 当设置 enable.auto.commit为false时，不会自动向kafka中保存消费者offset.需要异步的处理完数据之后手动提交
       */
      "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
    )

    //设置Kafka的topic
    val topics = Array("test")
    //创建与Kafka的连接，接收数据
    /*这里接收到数据的样子
    2019-09-26	1569487411604	1235	497	Kafka	Register
    2019-09-26	1569487411604	1235	497	Kafka	Register
    2019-09-26	1569487414838	390	   778	Flink	View
    */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,//
      Subscribe[String, String](topics, kafkaParams)
    )

    //对接收到的数据进行处理，打印出来接收到的key跟value，最后放回的是value
    val transStrem: DStream[String] = stream.map(record => {
      val key_value = (record.key, record.value)
      println("receive message key = "+key_value._1)
      println("receive message value = "+key_value._2)
      key_value._2
    }).filter(vl => vl.contains(","))


    //这里用了一下动态创建的Schema year_t
    //date_t
    val structType: StructType = StructType(List[StructField](
      StructField("year_t", StringType, nullable = true),
      StructField("date_t", StringType, nullable = true)
    ))

    //因为foreachRDD可以拿到封装到DStream中的rdd，可以对里面的rdd进行，
    /*代码解释：
        先从foreach中拿到一条数据，，在函数map中对接收来的数据用 “\n” 进行切分，放到Row中，用的是动态创建Schema，因为我们需要再将数据存储到hive中，所以需要Schema。
        因为map是transformance算子，所以用rdd.count（）触发一下
         spark.createDataFrame：创建一个DataFrame，因为要注册一个临时表，必须用到DataFrame
         frame.createOrReplaceTempView("t1")：注册临时表
           spark.sql("use spark")：使用 hive 的 spark 库
         result.write.mode(SaveMode.Append).saveAsTable("test_kafka")：将数据放到 test_kafka 中
    */
    transStrem.foreachRDD(one=>{
      val rdd : RDD[Row] = one.map({
        a =>
          val arr = a.toString.split(",")

          Row(arr(0).toString, arr(1).toString)
      })
      val frame : DataFrame = spark.createDataFrame(rdd,structType)
      //      println(" Scheme: "+frame.printSchema())

      frame.createOrReplaceTempView("t1")
      spark.sql("set hive.exec.dynamici.partition=true")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      //      spark.sql("select * from t1").show()
      val result = spark.sql("select * from t1")
      spark.sql("insert into test.jack select *,'2022-12-06' from t1")
//      result.write.mode(SaveMode.Append).insertInto("test.jack")
    }
    )



    /**
     * 以上业务处理完成之后，异步的提交消费者offset,这里将 enable.auto.commit 设置成false,就是使用kafka 自己来管理消费者offset
     * 注意这里，获取 offsetRanges: Array[OffsetRange] 每一批次topic 中的offset时，必须从 源头读取过来的 stream中获取，不能从经过stream转换之后的DStream中获取。
     */
    stream.foreachRDD { rdd =>
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}


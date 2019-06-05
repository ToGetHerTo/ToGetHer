package com.chenyuxin.consumer

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.KafkaConsumer
//scala与java的集合相互转换
import scala.collection.JavaConversions._
/**
  * @author chenshiliu
  * @create 2019-06-05 17:52
  */
class NewScalaConsumer {
  def cunsumer(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop18:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("test_v1"))
    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        println(record.offset() + "--" + record.key() + "--" + record.value())
      }
    }
    consumer.close()
  }
}

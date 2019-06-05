package com.chenyuxin.producer

import java.util.Properties
import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
/**
  * @author chenshiliu
  * @create 2019-06-05 17:19
  */
class NewScalaProducer {
  //配置属性
  def producer(): Unit ={
    val brokers_list = ""
    val topic = "jason_20180511"
    val properties = new Properties()
    properties.put("group.id", "jaosn_")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
    val producer = new KafkaProducer[String, String](properties)
    var num = 0
    for(i<- 1 to 1000){
      val json = new JSONObject()
      json.put("name","jason"+i)
      json.put("addr","25"+i)
      producer.send(new ProducerRecord(topic,json.toString()))
    }
    producer.close()
  }
}

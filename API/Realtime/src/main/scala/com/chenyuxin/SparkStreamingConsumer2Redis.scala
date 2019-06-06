package com.chenyuxin

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import com.chenyuxin.MyCaseClass


object SparkStreamingConsumer2Redis {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setAppName().setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(10))

    //通过自定义的MyKafkaUtil工具类创建KafkaStream
    val recordDstream : InputDStream[ConsumerRecord[String,String]] =
      MyKafkaUtil.getKafkaStream("kafka_topic",ssc)

    //将JSON字符串转成MyCaseClass对象
    val startupDstream: DStream[MyCaseClass] = recordDstream.map(_.value()).map {
      startupjson =>
        //将消费的JSON数据转为MyCaseClass类对应的格式
        val startup: MyCaseClass = JSON.parseObject(startupjson, classOf[MyCaseClass])
        startup
    }

    //将Redis中数据取出
    val startupFilterDstream: DStream[MyCaseClass] = startupDstream.transform {
      rdd =>
        //driver操作，将当前已经记录的数据取出
        val properties: Properties = MyPropertiesUtil.load("config.properties")
        val jedisDriver = new Jedis(properties.getProperty("redis.host"),
          properties.getProperty("redis.port").toInt)
        var dataSet: util.Set[String] = jedisDriver.smembers(
          "data" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
        //通过广播变量发送到executor
        val datasetBC: Broadcast[util.Set[String]] = sc.broadcast(dataSet)
        println("过滤前" + rdd.count())
        val rddFilterd: RDD[MyCaseClass] = rdd.filter {
          myCaseClass =>
            val datasetEx: util.Set[String] = datasetBC.value
            !datasetEx.contains(myCaseClass.mid)
        }
        println("过滤后" + rddFilterd.count())
        rddFilterd
    }

  }
}

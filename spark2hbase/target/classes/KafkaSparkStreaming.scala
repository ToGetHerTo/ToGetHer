import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Lin
  * @date 2019/5/14 - 10:35
  */
object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {
    // offset保存路径
    val checkpointPath = "/data/test.txt"

    val conf = new SparkConf()
      .setAppName("TestSpark")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //将类序列化


    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointPath)

    val bootstrapServers = "master:9092"
    val groupId = "root"
    val topics = Array("Test_spark")
    val maxPoll = 500

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      //这个代表，任务启动之前产生的数据也要读
      //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )

    val kafkaTopicDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
   // kafkaTopicDS.print()

    kafkaTopicDS.foreachRDD(rdd => {
      rdd.foreach(line => {
        println("offset=" + line.offset() + "  key=" + line.key() + "  value=" + line.value())
        hbase.hbase_api.putDataForRowkey("pie:test_spark",line.offset().toString,"info",line.key(),line.value())
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
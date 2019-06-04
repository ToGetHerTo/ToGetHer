package com.chenyuxin.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 15:48
 */
@SuppressWarnings("all")
//自动提交offset
public class NewConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //定义kafka服务的地址，不需要将所有broker指定上
        properties.put("bootstrap.servers", "hadoop102:9092");
        //指定consumer group
        properties.put("group.id", "test");
        //是否自动确认offset
        properties.put("enable.auto.commit", "true");
        //自动确认offset的时间间隔
        properties.put("auto.commit.interval","1000");
        //key的序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic1"));
        //死循环不停的从broker中拿数据
        while (true) {
            //读取数据，读取超时时间为100ms
            ConsumerRecords<String,String> consumerRecord = consumer.poll(100);

            for (ConsumerRecord<String, String> record : consumerRecord) {
                System.out.printf("offset=%d,key=%s,value=%s%n",record.offset(),record.key(),record.value());
            }
        }
    }
}

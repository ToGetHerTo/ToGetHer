package com.chenyuxin.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 17:45
 * 手动提交偏移量----按消费者提交
 *      通常从Kafka拿到的消息是要做业务处理，而且业务处理完成才算真正消费成功，所以需要客户端控制offset提交时间
 */
@SuppressWarnings("all")
public class NewConsumerManualOffser {
    public void munualCommit() {
        Properties properties = new Properties();
        //设置kafka集群的地址
        properties.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        properties.put("group.id", "my_group");
        //开启offset自动提交
        properties.put("enable.auto.commit", "false");
        //序列化器
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic1","topic2","topic3"));
        final int minBatchSize = 50;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                for (ConsumerRecord bf : buffer) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", bf.offset(), bf.key(), bf.value());
                }
                //当缓存中一批数据的数量大于minBatchSize时，提交offset，并清除缓存
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}

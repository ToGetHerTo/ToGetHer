package com.chenyuxin.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 19:05
 * 消费只读取特定分区数据-手动更改偏移量
 *      kafka Consumer Api还提供了自己存储offset的功能，将offset和data做到原子性，
 *      可以让消费具有Exactly Once 的语义，比kafka默认的At-least Once更强大
 *      设置消费者从自定义的位置开始拉取数据，比如从程序停止时已消费的下一Offset开始拉取数据，
 *      使用这个功能要求data和offset的update操作是原子的，否则可能会破坏数据一致性
 */
@SuppressWarnings("all")
public class NewConsumerPartitionDataControlOffset {
    /*
         手动设置指定分区的offset，只适用于使用Consumer.assign方法添加主题的分区，不适用于kafka自动管理消费者组中的消费者场景，
         后面这种场景可以使用ConsumerRebalanceListener做故障恢复使用
      */
    public void controlsOffset() {
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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //消费者订阅主题，并设置要拉取的分区​
        //加一段代码将自己保存的分区和偏移量读取到内存
        //load partition and it's offset
        TopicPartition partition0 = new TopicPartition("mytopic3", 0);
        consumer.assign(Arrays.asList(partition0));
        //告知Consumer每个分区应该从什么位置开始拉取数据，offset从你加载的值或者集合中拿
        consumer.seek(partition0, 4140L);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}

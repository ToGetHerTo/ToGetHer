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
 * @create 2019-06-04 18:18
 */
@SuppressWarnings("all")
/*
消费只读取特定分区数据，这种方式比上面的更加灵活，在实际应用场景中会经常使用
因为分区的数据是有序的，利用这个特性可以用于数据到达有先后顺序的业务，比如一个用户将订单提交，
紧接着又取消订单，那么取消的订单一定要后于提交的订单到达某一个分区，这样保证业务处理的正确性
​ 一旦指定了分区，要注意以下两点：
​ a.kafka提供的消费者组内的协调功能就不再有效
​ b.这样的写法可能出现不同消费者分配了相同的分区，为了避免偏移量提交冲突，每个消费者实例的group_id要不重复
 */
public class NewConsumerPartitionData {
    public void munualPollByPartition(){
        Properties props = new Properties();
        //设置kafka集群的地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        props.put("group.id", "my_group");
        //开启offset自动提交
        props.put("enable.auto.commit", "false");
        //序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题，并设置要拉取的分区
        TopicPartition partition0 = new TopicPartition("mytopic3", 0);
        //TopicPartition partition1 = new TopicPartition("mytopic2", 1);
        //consumer.assign(Arrays.asList(partition0, partition1));
        consumer.assign(Arrays.asList(partition0));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()){
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition()
                                + " , " + record.offset() +
                                ": " + record.value());
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

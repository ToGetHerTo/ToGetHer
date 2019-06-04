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
 * @create 2019-06-04 18:01
 */
@SuppressWarnings("all")
// 偏移量手动按分区提交
//更细粒度的提交数据，按照每个分区手动提交偏移量，这里实现了按照分区取数据，因此可以从分区入手，不同的分区可以做不同的操作，
// 可以灵活实现一些功能，为了验证手动提交偏移量，有两种方式：
// 1.debug的时候，在poll数据之后，手动提交前偏移量之前终止程序，再次启动看数据是否重复被拉取
// 2.debug的时候，在poll数据之后，手动提交前偏移量之前终止程序，登录Linux 主机执行如下命令：
//      kafka-consumer-groups.sh --bootstrap-server hadoop01:9092 --describe --group my_group
public class NewConsumerPartitionOffset {
    public void munualCommitByPartition() {
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
        //消费者订阅主题，可以订阅多个主题
        consumer.subscribe(Arrays.asList("mytopic3"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    //每个分区一个list，每个list放该分区的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("partition: " + partition.partition() + " , " + record.offset() + ": " + record.value());
                    }
                    //最后消费的数据list的长度-1，这是最后消费的数据，故最后偏移量为最后消费数据的offset值。
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    /*
                    提交的偏移量应该始终是您的应用程序将要读取的下一条消息的偏移量。因此，在调用commitSync（）时，
                    offset应该是处理的最后一条消息的偏移量加1
                    为什么这里要加上面不加喃？因为上面Kafka能够自动帮我们维护所有分区的偏移量设置，有兴趣的同学可以看看SubscriptionState.allConsumed()就知道
                     */
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}

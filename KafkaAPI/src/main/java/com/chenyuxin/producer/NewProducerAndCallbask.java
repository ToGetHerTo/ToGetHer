package com.chenyuxin.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 15:38
 */
@SuppressWarnings("all")
public class NewProducerAndCallbask {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka服务端的主机名和端口号
        properties.put("bootstrap.servers","hadoop18:9092");
        //等待所有副本节点的应答
        properties.put("acks", "all");
        //消息发送最大尝试次数
        properties.put("retries", 0);
        //一批消息处理大小
        properties.put("batch.size",16384);
        //请求延时
        properties.put("linger.ms", 1);
        //发送缓存区内存大小
        properties.put("buffer.memory", 33554432);
        //key序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 16; i++) {
            producer.send(
                    new ProducerRecord<String, String>(
                            "first", Integer.toString(i), "hello world-" + i),
                    new Callback() {
                        @Override
                        //还有一些其他参数
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (metadata != null) {
                                System.out.println(metadata.partition()+"---"+metadata.offset());
                            }
                        }
                    });
        }
        producer.close();

    }
}

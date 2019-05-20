package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Lin
 * @date 2019/5/13 - 15:05
 */

public class MyConsumer extends Thread {
    private String topic;
    KafkaConsumer<String, String> consumer;

    public MyConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaProperties.broker);
        properties.setProperty("group.id","root");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(KafkaProperties.topic));
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> recode: records) {
                System.out.println("Offset:"+recode.offset()+"  Key:"+recode.key()+"  Value:" + recode.value());
            }
        }
    }
    public static void main(String[] args){
        new MyConsumer().start();
    }
}
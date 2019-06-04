package com.chenyuxin.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 17:06
 */
@SuppressWarnings("all")
public class NewProducerTransactional {
    /**
     * 事务模式
     *   事务模式要求数据发送必须包含在事务中，在事务中可以向多个topic发送数据，消费者端最好也使用事务模式读，
     *   保证一次能将整个事务的数据全部读取过来。当然消费者也可以不设置为事务读的模式。
     */
    public void transactional(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("transactional.id", "my_transactional_id");
        Producer<String, String> producer = new KafkaProducer<>(props,
                new StringSerializer(), new StringSerializer());
        producer.initTransactions();
        try {
            //数据发送必须在beginTransaction()和commitTransaction()中间，否则会报状态不对的异常
            producer.beginTransaction();
            for (int i = 0; i < 100; i++){
                producer.send(new ProducerRecord<>("mytopic1", Integer.toString(i), Integer.toString(i)));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // 这些异常不能被恢复，因此必须要关闭并退出Producer
            producer.close();
        } catch (KafkaException e) {
            // 出现其它异常，终止事务
            producer.abortTransaction();
        }
        producer.close();
    }
}

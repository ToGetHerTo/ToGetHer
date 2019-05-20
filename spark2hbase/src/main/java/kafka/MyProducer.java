package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Lin
 * @date 2019/5/13 - 14:18
 */
public class MyProducer extends Thread {
    private String topic;
    private KafkaProducer<String, String> producer;

    public MyProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.broker);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run(){
        for (int i = 0; i < 100000; i++) {
            String[] data = new String[0];
            try {
                data = MockerData.mock().split(" ");
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(data[0].toString()+"  "+data[1].toString() );
            ProducerRecord<String , String> producerRecord = new ProducerRecord<String , String>("Test_spark",data[0].toString(),data[1].toString());
            producer.send(producerRecord);
        }
        producer.close();

    }




    public static void main(String[] args) {
        new MyProducer(KafkaProperties.topic).start();
    }
}
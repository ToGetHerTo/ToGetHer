package com.chenyuxin.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenshiliu
 * @create 2019-06-05 10:27
 * Kafka是线程不安全的，kafka官方提供了一种写法来避免线程安全问题
 */
@SuppressWarnings("all")
public class NewConsumerRunner implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch latch;

    public NewConsumerRunner(KafkaConsumer<String, String> consumer, CountDownLatch latch) {
        this.consumer = consumer;
        this.latch = latch;
    }

    @Override

    public void run() {
        System.out.println("threadName..." + Thread.currentThread().getName());
        try {
            consumer.subscribe(Arrays.asList("mytopic3"));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("threadName= %s, offset = %d, key = %s, value = %s%n",
                            Thread.currentThread().getName(),
                            record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        }finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown(){
        System.out.println("close ConsumerRunner");
        closed.set(true);
        consumer.wakeup();
    }
    //驱动方法
    public void autoCommitParallelTest() {
        Properties properties = new Properties();
        //设置kafka集群的地址
        properties.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //设置消费者组，组名字自定义，组名字相同的消费者在一个组
        properties.put("group.id", "my_group");
        //开启offset自动提交
        properties.put("enable.auto.commit", "true");
        //自动提交时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        //序列化器
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //实例化一个消费者
        final List<NewConsumerRunner> consumers = new ArrayList<>();
        final List<KafkaConsumer<String, String>> kafkaConsumers = new ArrayList<>();
        for(int i = 0;i < 2;i++){
            kafkaConsumers.add(new KafkaConsumer<String, String>(properties));
        }
        final CountDownLatch latch = new CountDownLatch(2);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        for(int i = 0;i < 2;i++) {
            NewConsumerRunner c = new NewConsumerRunner(kafkaConsumers.get(i), latch);
            consumers.add(c);
            executor.submit(c);
        }
        /*
            这个方法的意思就是在jvm中增加一个关闭的钩子，当jvm关闭的时候，
            会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子，当系统执行完这些钩子后，jvm才会关闭
            所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁、关闭连接等操作
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("....................");
                for (NewConsumerRunner consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

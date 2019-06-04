package com.chenyuxin.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author chenshiliu
 * @create 2019-06-04 15:00
 */
@SuppressWarnings("all")
public class NewProducer {
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
                            "first", Integer.toString(i), "hello world-" + i));
        }
        producer.close();

        //---------------------------------------------------------------------------------------------
        //以下参数，供君选择
        properties.put("bootstrap.servers",
                "kafka集群的broker-list，如：hadoop01:9092,hadoop02:9092");
        properties.put("acks",
                "确保生产者可靠性设置，0:不等待成功返回，1:等Leader写成功返回，all:Follower写成功返回,可用-1代替,且-1为默认值");
        properties.put("key.serializer",
                "key的序列化器");
        properties.put("value.serializer",
                "value的序列化器");
        properties.put("buffer.memory",
                "Producer总体内存大小，默认值33554432");
        properties.put("compression.type",
                "压缩类型，压缩最好用于批量处理，批量处理消息越多，压缩性能越好，默认无");
        properties.put("retries",
                "发送失败尝试重发次数，默认值为0");
        properties.put("batch.size",
                "每个partition的未发送消息大小，默认16384");
        properties.put("client.id",
                "附着在每个请求的后面，用于标识请求是从什么地方发送过来的");
        properties.put("onnections.max.idle.ms",
                "连接空闲时间超过过久自动关闭（单位毫秒），默认540000");
        properties.put("linger.ms",
                "数据在缓冲区中保留的时长,0表示立即发送为了减少网络耗时，需要设置这个值太大可能容易导致缓冲区满，阻塞消费者太小容易频繁请求服务端,默认值为0");
        properties.put("max.block.ms",
                "最大阻塞时长，默认值为60000");
        properties.put("max.request.size",
                "请求的最大字节数，该值要比batch.size大不建议去更改这个值，如果设置不好会导致程序不报错，但消息又没有发送成功，默认值为1048576");
        properties.put("partitioner.class",
                "分区类，可以自定义分区类，实现partitioner接口，默认为按哈希值%partitions分区");
        properties.put("receive.buffer.bytes",
                "socket的接收缓存空间大小,当阅读数据时使用，默认32768");
        properties.put("request.timeout.ms",
                "等待请求响应的最大时间,超时则重发请求,超过重试次数将抛异常，默认值3000");
        properties.put("send.buffer.bytes",
                "发送数据时的缓存空间大小,131072");
        properties.put("timeout.ms",
                "控制server等待来自followers的确认的最大时间，30000");
        properties.put("max.in.flight.requests.per.connection",
                "kafka可以在一个connection中发送多个请求，叫作一个flight,这样可以减少开销，但是如果产生错误，可能会造成数据的发送顺序改变。默认值为5");
        properties.put("metadata.fetch.timeout.ms",
                "从ZK中获取元数据超时时间比如topic\\host\\partitions，默认值为60000");
        properties.put("metadata.max.age.ms",
                "即使没有任何partition leader 改变，强制更新metadata的时间间隔，默认值为300000");
        properties.put("metric.reporters",
                "类的列表，用于衡量指标。实现MetricReporter接口，将允许增加一些类，这些类在新的衡量指标产生时就会改变。JmxReporter总会包含用于注册JMX统计");
        properties.put("metrics.num.samples",
                "用于维护metrics的样本数，默认值为2");
        properties.put("metrics.sample.window.ms",
                "metrics系统维护可配置的样本数量，在一个可修正的window size。这项配置配置了窗口大小，例如。我们可能在30s的期间维护两个样本。当一个窗口推出后，我们会擦除并重写最老的窗口，默认值为30000");
        properties.put("reconnect.backoff.ms",
                "连接失败时，当我们重新连接时的等待时间。这避免了客户端反复重连，默认值为10");
        properties.put("retry.backoff.ms",
                "在试图重试失败的produce请求之前的等待时间。避免陷入发送-失败的死循环中，默认值为100");
        //关于Kafka更全的文档地址
        String documentip = "http://kafka.apache.org/documentation/#producerconfigs";
        //幂等模式
        /*
        幂等性：客户端一次或多次操作，最终数据是一致的，比如购买火车票支付时可能显示网络异常，但其实已经扣款成功，
        用户再次发起扣款不会再触发真正的扣款Kafka只能保证在一个会话中的幂等性
        幂等模式只需要将enable.idempotence设置为true，一旦设置了该属性，那么retries默认是Integer.MAX_VALUE ，
        acks默认是all。代码的写法和前面例子没什么区别
        */
        //---------------------------------------------------------------------------------------------
    }
}

package com.chenyuxin.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chenshiliu
 * @create 2019-06-05 15:58
 * kafka提供了这个监听来处理分区的变化，
 *      区分被取消时调用onPartitionsRevoked方法，
 *      分区被分配时调用onPartitionsAssigned方法。
 */
@SuppressWarnings("all")
public class NewConsumerRebalance implements ConsumerRebalanceListener {
    static Map<TopicPartition,Long> partitionMap = new ConcurrentHashMap<>();
    private Consumer<?,?> consumer;
    //实例化Listener的时候将Consumer传进来
    public NewConsumerRebalance(Consumer<?,?> consumer) {
        this.consumer = consumer;
    }
    /*
        有新的消费者加入消费者组或者已有消费者从消费者组中移除会触发kafka的rebalance机制，rebalance被调用前会先调用下面的方法
        此时你可以将分区和它的偏移量记录到外部存储中，比如DBMS、文件、缓存数据库等，还可以在这里处理自己的业务逻辑
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            //记录分区和它的偏移量
            partitionMap.put(partition, consumer.position(partition));
            //清空缓存​
            System.out.println("onPartitionsRevoked partition:" + partition.partition() + " - offset" + consumer.position(partition));
        }
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        //设置分区的偏移量
        for(TopicPartition partition: partitions){
            System.out.println("onPartitionsAssigned partition:" + partition.partition()+" - offset"+consumer.position(partition));
            if(partitionMap.get(partition)!=null){
                consumer.seek(partition, partitionMap.get(partition));
            }else{
                //自定义处理逻辑
            }
        }
    }
}

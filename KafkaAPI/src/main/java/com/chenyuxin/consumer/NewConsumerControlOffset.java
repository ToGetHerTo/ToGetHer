package com.chenyuxin.consumer;

/**
 * @author chenshiliu
 * @create 2019-06-04 19:05
 */
@SuppressWarnings("all")
/*
 ​kafka Consumer Api还提供了自己存储offset的功能，将offset和data做到原子性，
 可以让消费具有Exactly Once 的语义，比kafka默认的At-least Once更强大
 消费者从指定分区拉取数据-手动更改偏移量
 ​设置消费者从自定义的位置开始拉取数据，比如从程序停止时已消费的下一Offset开始拉取数据，
 使用这个功能要求data和offset的update操作是原子的，否则可能会破坏数据一致性
 */
public class NewConsumerControlOffset {

}

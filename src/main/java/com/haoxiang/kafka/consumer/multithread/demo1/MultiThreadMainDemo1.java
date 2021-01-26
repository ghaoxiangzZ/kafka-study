package com.haoxiang.kafka.consumer.multithread.demo1;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerMain.java
 * @Description 多consumer实例多线程消费
 * @createTime 2021年01月26日 15:07:00
 */
public class MultiThreadMainDemo1 {

    public static void main(String[] args) {
        String brokerList = "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}

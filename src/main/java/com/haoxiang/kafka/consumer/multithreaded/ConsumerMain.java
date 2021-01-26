package com.haoxiang.kafka.consumer.multithreaded;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerMain.java
 * @Description 多线程消费测试
 * @createTime 2021年01月26日 15:07:00
 */
public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}

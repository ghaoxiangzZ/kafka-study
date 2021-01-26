package com.haoxiang.kafka.consumer.multithread.demo1;

import java.util.ArrayList;
import java.util.List;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerGroup.java
 * @Description 多线程消费组
 * @createTime 2021年01月26日 15:01:00
 */
public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }
}

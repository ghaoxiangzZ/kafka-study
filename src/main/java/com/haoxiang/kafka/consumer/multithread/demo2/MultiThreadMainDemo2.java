package com.haoxiang.kafka.consumer.multithread.demo2;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName MultiThreadMainDemo2.java
 * @Description 1 consumer实例，多worker消费的模式
 * @createTime 2021年01月26日 16:31:00
 */
public class MultiThreadMainDemo2 {

    public static void main(String[] args) {
        String brokerList = "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094";
        String groupId = "testGroup1";
        String topic = "test-topic";
        final ConsumerThreadHandler<byte[], byte[]> handler = new ConsumerThreadHandler<>(brokerList, groupId, topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        new Thread(() -> handler.consume(cpuCount)).start();
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("starting to close the consumer......");
        handler.close();
    }
}

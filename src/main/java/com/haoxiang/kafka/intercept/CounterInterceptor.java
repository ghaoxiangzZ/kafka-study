package com.haoxiang.kafka.intercept;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName CounterInterceptor.java
 * @Description 打印计数器拦截器
 * @createTime 2021年01月20日 15:28:00
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int errorCounter = 0;
    private int successCounter = 0;

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }


    @Override
    public void close() {
        // 打印计数
        System.out.println("Successful send:" + successCounter);
        System.out.println("Failed send:" + errorCounter);
    }


}

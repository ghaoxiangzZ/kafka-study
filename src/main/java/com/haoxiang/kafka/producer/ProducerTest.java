package com.haoxiang.kafka.producer;

import com.haoxiang.kafka.pojo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ProducerTest.java
 * @Description 各种生产者demo
 * @createTime 2021年01月19日 18:49:00
 */
public class ProducerTest {

    private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    /*
     * @title testProducerAsyncSend
     * @description 测试简单异步发送逻辑
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-20 15:48:05
     * @return void
     * @throws
     */
    private static void testProducerAsyncSend() {
        String topic = "test-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            String msg = Integer.toString(i);
            producer.send(new ProducerRecord<>("test-topic", msg, msg), (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), msg);
                } else {
                    logger.info("kafka发送msg={}失败", msg, e);
                }
            });
        }
        producer.close();
    }

    /*
     * @title testCustomizablePartition
     * @description 测试自定义分区异步发送
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-20 15:48:20
     * @return void
     * @throws
     */
    private static void testCustomizablePartition() {
        String topic = "test-customizable-partition-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.haoxiang.kafka.partitioner.AuditPartitioner");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            String key = i % 10 == 0 ? "audit" : "other";
            String value = "audit record";
            producer.send(new ProducerRecord<>(topic, key, value), (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), value);
                } else {
                    logger.info("kafka发送msg={}失败", value, e);
                }
            });
        }
        producer.close();
    }

    /*
     * @title testCustomizableSerializer
     * @description 测试自定义序列化方式异步发送
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-20 15:48:40
     * @return void
     * @throws
     */
    private static void testCustomizableSerializer() {
        String topic = "test-customizable-serializer-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        Producer<String, User> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            User user = new User("haoxiang", "guo", 26, "shenzhen");
            producer.send(new ProducerRecord<>(topic, user), (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), user);
                } else {
                    logger.info("kafka发送msg={}失败", user, e);
                }
            });
        }
        producer.close();
    }

    /*
     * @title testSendWithInterceptor
     * @description 测试带自定义拦截器异步发送
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-20 15:49:20
     * @return void
     * @throws
     */
    private static void testSendWithInterceptor() {
        String topic = "test-interceptor-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        // 构建拦截器链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.haoxiang.kafka.intercept.TimeStampInterceptor");
        interceptors.add("com.haoxiang.kafka.intercept.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            String value = "message" + i;
            producer.send(new ProducerRecord<>(topic, value), (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), value);
                } else {
                    logger.info("kafka发送msg={}失败", value, e);
                }
            });
        }
        producer.close();
    }

    public static void main(String[] args) {
        testProducerAsyncSend();
    }

}

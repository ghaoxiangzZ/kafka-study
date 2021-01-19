package com.haoxiang.kafka.producer;

import com.haoxiang.kafka.pojo.User;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName Producer.java
 * @Description TODO
 * @createTime 2021年01月19日 18:49:00
 */
public class TestProducer {

    private static Logger logger = LoggerFactory.getLogger(TestProducer.class);

    private static void testProducerAsyncSend() {
        String topic = "test-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            String msg = Integer.toString(i);
            producer.send(new ProducerRecord<>("test-topic", msg, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), msg);
                    } else {
                        logger.info("kafka发送msg={}失败", msg, e);
                    }
                }
            });
        }
        producer.close();
    }

    private static void testCustomizablePartition() {
        String topic = "test-customizable-partition-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.haoxiang.kafka.partitioner.AuditPartitioner");
        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            String key = i % 10 == 0 ? "audit" : "other";
            String value = "audit record";
            producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), value);
                    } else {
                        logger.info("kafka发送msg={}失败", value, e);
                    }
                }
            });
        }
        producer.close();
    }

    private static void testCustomizableSerializer() {
        String topic = "test-customizable-serializer-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.haoxiang.kafka.serializer.UserSerializer");
        props.put("acks", "-1");
        props.put("retries", "3");
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            User user = new User("haoxiang", "guo", 26, "shenzhen");
            ProducerRecord<String, User> producerRecord = new ProducerRecord(topic, user, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("topic={}, partition={}, time={}发送msg={}成功", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.timestamp(), user);
                    } else {
                        logger.info("kafka发送msg={}失败", user, e);
                    }
                }
            });
        }
        producer.close();
    }

    public static void main(String[] args) {
        testCustomizablePartition();
    }

}

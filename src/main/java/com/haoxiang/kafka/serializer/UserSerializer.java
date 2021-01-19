package com.haoxiang.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName UserSerializer.java
 * @Description 自定义kafka序列化
 * @createTime 2021年01月19日 22:06:00
 */
public class UserSerializer implements Serializer {

    private Logger logger = LoggerFactory.getLogger(UserSerializer.class);

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(o).getBytes("utf-8");
        } catch (Exception e) {
            logger.warn("failed to serializer the object:{}", o, e);
        }
        return ret;
    }

    @Override
    public void close() {

    }
}

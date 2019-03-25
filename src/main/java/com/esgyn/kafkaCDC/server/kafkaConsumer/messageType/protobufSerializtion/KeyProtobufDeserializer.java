package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Key;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 * @author xzc
 * the Deserializer of the Key
 * 
 */

public class KeyProtobufDeserializer implements Deserializer<Key> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Key deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Key key = null;
        try {
            key = MessageDb.Key.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return key;
    }

    @Override
    public void close() {

    }

}

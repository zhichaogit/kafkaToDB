package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Record;
import com.google.protobuf.InvalidProtocolBufferException;



/**
 * 
 * @author xzc
 * the Deserializer of the message
 *
 */
public class RecordProtobufDeserializer implements Deserializer<Record> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub

    }

    @Override
    public Record deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Record ans = null;
        try {
            ans = MessageDb.Record.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return ans;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }



}

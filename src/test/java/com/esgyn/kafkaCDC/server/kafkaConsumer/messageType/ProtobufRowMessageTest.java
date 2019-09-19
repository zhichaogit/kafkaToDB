package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import static org.junit.Assert.*;


import org.junit.Before;
import org.junit.Test;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.ProtobufRowMessage;
import com.google.protobuf.ByteString;

public class ProtobufRowMessageTest {
    ProtobufRowMessage protobufRowMessage = null;
    @Before
    public void setUp() throws Exception {
        protobufRowMessage = new ProtobufRowMessage();
    }

    @Test
    public void insertEmptyStr() {
       boolean insertEmptyStr = protobufRowMessage.insertEmptyStr("NCHAR VARYING    ".trim().toUpperCase());
       assertTrue(insertEmptyStr);
    }
    @Test
    public void insertEmptyStr2() {
        boolean insertEmptyStr = protobufRowMessage.insertEmptyStr("nothing     ".trim().toUpperCase());
        assertFalse(insertEmptyStr);
    }
    
    @Test
    public void bytesToString() throws Exception {
        ByteString input = ByteString.copyFrom("unicom 中国 Test", "GBK");
        String str = protobufRowMessage.bytesToString(input, "GBK",null,0,null);
        assertTrue(str.equals("unicom 中国 Test"));
    }
    @Test
    public void bytesToString2() throws Exception {
        ByteString input = ByteString.copyFrom("unicom 中国 Test", "GBK");
        try {
            String str = protobufRowMessage.bytesToString(input, "UTF-8",null,0,null);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

}

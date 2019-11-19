package com.esgyn.kafkaCDC.server.clientServer;

import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Message implements Serializable {
    private static Logger log = Logger.getLogger(Message.class);
    
    @Getter
    @Setter
    protected int            msgType    = 0;
    @Getter
    @Setter 
    protected int            subType    = 0;
    @Getter
    @Setter
    protected String         msgs       = "";

    public final static int INVALID    = 0;

    // req msg type
    public final static int  SHUTDOWN   = 1;
    public final static int  START      = 2;
    public final static int  STOP       = 3;
    public final static int  PRINT      = 4;
    
    // feedback msg type
    public final static int  RETURN     = 6;


    // sub message type
    // Shutdown
    public final static int  NORMAL     = 101;
    public final static int  IMMEDIATE  = 102;
    public final static int  ABORT      = 103;

    // start and STOP sub type 
    public final static int  CONSUMER   = 104;
    public final static int  LOADER     = 105;

    // print sub type 
    public final static int  CONSUMERS  = 106;
    public final static int  LOADERS    = 107;
    public final static int  TABLES     = 108;
    public final static int  TASKS      = 109;

    // the return values
    public final static int  SUCCEED    = 110;
    public final static int  FAIL       = 111;

    public Message() {}

    public Message(int msgTYpe, int subType) {
	this.msgType = msgType;
	this.subType = subType;
    }

    public boolean init(String msgType, String subType) {
	String typeUpper    = msgType.toUpperCase();
	String subTypeUpper = subType.toUpperCase();

        this.msgType = getMsgType(typeUpper);
	this.subType = getSubType(subTypeUpper);
	switch(this.msgType){
	case SHUTDOWN:
	    switch (this.subType) {
	    case NORMAL:
	    case IMMEDIATE:
	    case ABORT:
		break;

	    default:
		report_error_and_exit(typeUpper, subTypeUpper);
	    }
	    break;

	case START:
	    switch (this.subType) {
	    case CONSUMER:
	    case LOADER:
		break;

	    default:
		report_error_and_exit(typeUpper, subTypeUpper);
		return false;
	    }
	    break;

	case STOP:
	    switch (this.subType) {
	    case CONSUMER:
	    case LOADER:
		break;

	    default:
		report_error_and_exit(typeUpper, subTypeUpper);
		return false;
	    }
	    break;

	case PRINT:
	    switch (this.subType) {
	    case CONSUMERS:
	    case LOADERS:
	    case TABLES:
	    case TASKS:
		break;

	    default:
		report_error_and_exit(typeUpper, subTypeUpper);
		return false;
	    }
	    break;

	default:
	    report_error_and_exit(typeUpper, subTypeUpper);
	    return false;
        }

	return true;
    }


    final static int getMsgType(String type) {
	switch (type) {
	case "SHUTDOWN":
	    return SHUTDOWN;

	case "START":
	    return START;

	case "STOP":
	    return STOP;

	case "PRINT":
	    return PRINT;

	case "RETURN":
	    return RETURN;
	}

	return INVALID;
    }


    final static int getSubType(String type) {
	switch (type) {
	case "NORMAL":
	    return NORMAL;

	case "IMMEDIATE":
	    return IMMEDIATE;

	case "ABORT":
	    return ABORT;

	case "CONSUMER":
	    return CONSUMER;

	case "LOADER":
	    return LOADER;

	case "CONSUMERS":
	    return CONSUMERS;

	case "LOADERS":
	    return LOADERS;

	case "TABLES":
	    return TABLES;

	case "TASKS":
	    return TASKS;

	case "SUCCEED":
	    return SUCCEED;

	case "FAIL":
	    return FAIL;
	}

	return INVALID;
    }
    
    public byte[] getBytes() {
        byte[] buffer = null;

        ByteArrayOutputStream baos = null;
	ObjectOutputStream    oos  = null;
        try {
	    baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);

	    oos.writeObject(this);
	    oos.flush();
	    buffer = baos.toByteArray();
	} catch (Exception e) {
	    log.error("failed to serialize message", e);
        } finally {
	    try {
		if (oos != null)
		    oos.close();

		if (baos != null)
		    baos.close();
	    } catch (Exception e) {
	    }
        }

	byte[] msgBytes = new byte[buffer.length + 4];
	msgBytes[0] = (byte)((buffer.length >> 24) & 0xFF);
	msgBytes[1] = (byte)((buffer.length >> 16) & 0xFF);
	msgBytes[2] = (byte)((buffer.length >> 8) & 0xFF);
	msgBytes[3] = (byte)((buffer.length) & 0xFF);
	System.arraycopy(buffer, 0, msgBytes, 4, buffer.length);

	return msgBytes;
    }

    public static final Message decodeBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = null;
	ObjectInputStream    ois  = null;

        try {
	    bais = new ByteArrayInputStream(bytes, 0, bytes.length);
	    ois  = new ObjectInputStream(bais);
	    return(Message) ois.readObject();
        } finally {
	    if (ois != null)
		ois.close();

	    if (bais != null)
		bais.close();
        }
    }

    void report_error_and_exit(String type, String subType) {
	System.err.println("the input type [" + type + "], sub type [" 
			   + subType + "] are not supported.");
	System.exit(0);
    }

    public String toString() {
	return "type: " + msgType + ", sub type: " + subType + ", msg: " + msgs;
    }
}

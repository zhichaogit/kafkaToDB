package com.esgyn.kafkaCDC.server.clientServer;

import java.nio.channels.*;
import java.io.IOException;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerTasks;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Utils;

public class KCServer extends Thread{
    private static Logger log = Logger.getLogger(KCServer.class);

    private final AtomicBoolean running = new AtomicBoolean(true);
    public final static boolean TCPNODELAY = true;
    public final static boolean TCPKEEPLIVE = false;
    

    private int                 port     = 0;
    private ConsumerTasks       consumerTasks = null;

    private Selector            selector = null;
    private ServerSocketChannel channel  = null;

    private KCConnection        kcConn   = null;

    public static final Message SUCCEED = new Message(Message.RETURN, Message.SUCCEED);
    public static final Message FAIL    = new Message(Message.RETURN, Message.FAIL);

    public KCServer(ConsumerTasks consumerTasks, int port) {
        this.consumerTasks = consumerTasks;
	this.port = port;

	init();
    }

    public void init() {
        try {
	    selector = Selector.open();
	    // create a server socket channel and bind to port
	    channel = ServerSocketChannel.open();
	    channel.configureBlocking(false);
	    InetSocketAddress isa = new InetSocketAddress(port);
	    channel.socket().bind(isa);

	    // register interest in Connection Attempts by clients
	    channel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (Exception e) {
            log.error("failed to start KafkaCDC Listener!", e);
        }
    }

    public void run() {
	log.debug("KafkaCDC KCServer is running");
	while (running.get()) {
	    if (log.isDebugEnabled()) {
		log.debug("Waiting for client to connect");
	    }

	    try {
		if (selector.select(1000) > 0) {
		    Set<SelectionKey> readyKeys = selector.selectedKeys();
		    process(readyKeys.iterator());
		}
	    } catch (Exception e) {
		log.error(e.getMessage(), e);
	    }
	}
	log.info("kcListener close");
    }

    private void process(Iterator<SelectionKey> readyIter) throws IOException {
	String msg = null;

	while (readyIter.hasNext()) {
	    // get the key
	    SelectionKey key = (SelectionKey) readyIter.next();

	    // remove the current key
	    readyIter.remove();

	    if (key.isAcceptable()) {
		// new connection attempt
		SocketChannel xServer  = getSocketChannel(key);
		KCConnection  kcConn   = null;
		Message       feedback = null;
		Message       require  = null;

		SUCCEED.setMsgs("");
		if (log.isDebugEnabled()) {
		    log.debug("client connection request arrived");
		}

		try {
		    if (xServer.isConnectionPending()) {
			xServer.finishConnect();
		    }

		    kcConn   = new KCConnection(xServer);
		    require  = kcConn.receive();
		    feedback = processKCMessage(require);

		    if (log.isDebugEnabled()) {
			log.debug("client connection request established");
		    }
		} catch (Exception e) {
		    feedback = FAIL;
		    msg = e.toString();
		    feedback.setMsgs(msg);
		} finally {
		    try {
			kcConn.send(feedback);

			// wait ask to exit
			require = kcConn.receive();
			if (require.getMsgType() != Message.CLOSE 
			    || require.getSubType() != Message.DISCONNECT)
			    log.error("client message type [" + require.getMsgType() 
				      + "], sub type [" + require.getSubType() + "] error");
			
			xServer.close();
		    } catch (Exception e) {
			log.error(e);
		    }
		}
	    }
	} // end while readyIter
    }

    public SocketChannel getSocketChannel(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

        SocketChannel xServer = serverChannel.accept();
        //xServer.configureBlocking(false);
        xServer.socket().setTcpNoDelay(TCPNODELAY);
        xServer.socket().setKeepAlive(TCPKEEPLIVE);
        xServer.socket().setSoTimeout(1000);
        return xServer;
    }

    public Message processKCMessage(Message kcMessage) {
	Message retMsg  = null;
	int     msgType = kcMessage.getMsgType();

        switch (msgType) {
	case Message.SHUTDOWN:
	    retMsg = processShutdown(kcMessage);
	    break;

	case Message.START:
	    retMsg = processStart(kcMessage);
	    break;

	case Message.STOP:
	    retMsg = processStop(kcMessage);
	    break;

	case Message.PRINT:
	    retMsg = processPrint(kcMessage);
	    break;

	default:
	    String errMsg = "Unexpected kind of message type: " + msgType;
	    log.error(errMsg);
	    retMsg = FAIL;
	    retMsg.setMsgs(errMsg);
	    break;
        }

	return retMsg;
    }

    public Message processShutdown(Message kcMessage) {
	Message retMsg  = SUCCEED;
	String  result  = null;
	int     subType = kcMessage.getSubType();

        switch (subType) {
	case Message.NORMAL:
	    consumerTasks.close(Constants.KAFKA_CDC_NORMAL);
	    result = "Shutdown with client normal command and succeed!";
	    break;

	case Message.IMMEDIATE:
	    consumerTasks.close(Constants.KAFKA_CDC_IMMEDIATE);
	    result = "Shutdown with client immediate command and succeed!";
	    break;

	case Message.ABORT:
	    consumerTasks.close(Constants.KAFKA_CDC_ABORT);
	    result = "Shutdown with client abort command and succeed!";
	    break;

	default:
	    result = "Unexpected kind of message sub type: " + subType;
	    retMsg = FAIL;
	    retMsg.setMsgs(result);
	    log.error(result);

	    return retMsg;
        }

	log.info(result);
	retMsg.setMsgs(result);

	return retMsg;
    }

    public Message processStart(Message kcMessage) {
	Message retMsg  = SUCCEED;
	String  result  = null;
	int     subType = kcMessage.getSubType();

        switch (subType) {
	case Message.CONSUMER:
	    result = "start consumer with client command and succeed!";
	    // TODO
	    break;

	case Message.LOADER:
	    result = "start loader with client command and succeed!";
	    // TODO
	    break;

	default:
	    result = "Unexpected kind of message sub type: " + subType;
	    retMsg = FAIL;
	    retMsg.setMsgs(result);
	    log.error(result);
	    return retMsg;
        }

	retMsg.setMsgs(result);
	log.info(result);

	return retMsg;
    }

    public Message processStop(Message kcMessage) {
	Message retMsg  = SUCCEED;
	String  result  = null;
	int     subType = kcMessage.getSubType();

        switch (subType) {
	case Message.CONSUMER:
	    result = "start consumer with client command and succeed!";
	    // TODO
	    break;

	case Message.LOADER:
	    result = "start loader with client command and succeed!";
	    // TODO
	    break;

	default:
	    result = "Unexpected kind of message sub type: " + subType;
	    retMsg = FAIL;
	    retMsg.setMsgs(result);
	    log.error(result);
	    
	    return retMsg;
        }
	
	retMsg.setMsgs(result);
	log.info(result);

	return retMsg;
    }

    public Message processPrint(Message kcMessage) {
	Message      retMsg    = SUCCEED;
	String       result    = null;
	int          subType   = kcMessage.getSubType();
	StringBuffer strBuffer = new StringBuffer();

        switch (subType) {
	case Message.CONSUMERS:
	    consumerTasks.showConsumers(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    result = "print consumer state with client command and succeed!";
	    break;

	case Message.LOADERS:
	    consumerTasks.getLoaderTasks()
		.showLoaders(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    result = "print loader state with client command and succeed!";
	    break;

	case Message.TABLES:
	    consumerTasks.getLoaderTasks().getLoadStates()
		.showTables(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    result = "print tables state with client command and succeed!";
	    break;
	    
	case Message.TASKS:
	    result = "print tasks with client command and succeed!";
	    consumerTasks.showTasks(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    break;

	case Message.KAFKA:
	    result = "print kafka state with client command and succeed!";
	    consumerTasks.getConsumeStates()
		.showStates(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    break;

	case Message.DATABASE:
	    result = "print database state with client command and succeed!";
	    consumerTasks.getLoaderTasks().getLoadStates()
		.showDatabase(strBuffer, Constants.KAFKA_JSON_FORMAT);
	    break;
	    
	default:
	    String errMsg = "Unexpected kind of message sub type: " + subType;
	    log.error(errMsg);
	    retMsg = FAIL;
	    retMsg.setMsgs(errMsg);
            
	    return retMsg;
        }

	retMsg.setMsgs(strBuffer.toString());
	log.info(result);

	return retMsg;
    }

    public synchronized void stopServer() {
        if (log.isTraceEnabled()) { log.trace("enter");}
        running.set(false);
        log.info("kcListener to close");
        if (log.isTraceEnabled()) { log.trace("exit");}
    }
}

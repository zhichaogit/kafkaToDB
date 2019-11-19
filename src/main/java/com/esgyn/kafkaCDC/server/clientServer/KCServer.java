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
	while (readyIter.hasNext()) {
	    // get the key
	    SelectionKey key = (SelectionKey) readyIter.next();

	    // remove the current key
	    readyIter.remove();

	    if (key.isAcceptable()) {
		// new connection attempt
		SocketChannel xServer = getSocketChannel(key);
		if (log.isDebugEnabled()) {
		    log.debug("client connection request arrived");
		}

		try {
		    if (xServer.isConnectionPending()) {
			xServer.finishConnect();
		    }

		    KCConnection kcConn = new KCConnection(xServer);
		    Message require   = kcConn.receive();
		    Message feedback  = processKCMessage(require);
		    kcConn.send(feedback);

		    if (log.isDebugEnabled()) {
			log.debug("client connection request established");
		    }
		} catch (Exception e) {
		    try {
			xServer.close();
		    } catch (IOException ioe) {
			log.error(ioe);
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
                log.error("Unexpected kind of message type: " + msgType);
		break;
        }

	return retMsg;
    }

    public Message processShutdown(Message kcMessage) {
	Message retMsg  = SUCCEED;
	int     subType = kcMessage.getSubType();

        switch (subType) {
            case Message.NORMAL:
                log.info("shutdown with client normal command");
                consumerTasks.close(Constants.KAFKA_CDC_NORMAL);
                break;

            case Message.IMMEDIATE:
                log.info("shutdown with client immediate command");
                consumerTasks.close(Constants.KAFKA_CDC_IMMEDIATE);
                break;

            case Message.ABORT:
                log.info("shutdown with client abort command");
                consumerTasks.close(Constants.KAFKA_CDC_ABORT);
                break;

            default:
                log.error("Unexpected kind of message sub type: " + subType);
                retMsg = FAIL;
                break;
        }

	return retMsg;
    }

    public Message processStart(Message kcMessage) {
	Message retMsg  = SUCCEED;
	int     subType = kcMessage.getSubType();

        switch (subType) {
            case Message.CONSUMER:
                log.info("start consumer with client command");
		// TODO
                break;

            case Message.LOADER:
                log.info("start loader with client command");
		// TODO
                break;

            default:
                log.error("Unexpected kind of message sub type: " + subType);
                retMsg = FAIL;
                break;
        }

	return retMsg;
    }

    public Message processStop(Message kcMessage) {
	Message retMsg  = SUCCEED;
	int     subType = kcMessage.getSubType();

        switch (subType) {
            case Message.CONSUMER:
                log.info("start consumer with client command");
		// TODO
                break;

            case Message.LOADER:
                log.info("start loader with client command");
		// TODO
                break;

            default:
                log.error("Unexpected kind of message sub type: " + subType);
                retMsg = FAIL;
                break;
        }

	return retMsg;
    }

    public Message processPrint(Message kcMessage) {
	Message      retMsg    = SUCCEED;
	int          subType   = kcMessage.getSubType();
	StringBuffer strBuffer = new StringBuffer();

        switch (subType) {
            case Message.CONSUMER:
                log.info("print consumer state with client command");
		consumerTasks.showConsumers(strBuffer, Constants.KAFKA_JSON_FORMAT);
                break;

            case Message.LOADER:
                log.info("print loader state with client command");
		consumerTasks.getLoaderTasks()
		    .showLoaders(strBuffer, Constants.KAFKA_JSON_FORMAT);
                break;

            case Message.TABLES:
                log.info("print tables state with client command");
		consumerTasks.getLoaderTasks().getLoadStates()
		    .showTables(strBuffer, Constants.KAFKA_JSON_FORMAT);
                break;

            case Message.TASKS:
                log.info("print tasks with client command");
		consumerTasks.showTasks(strBuffer, Constants.KAFKA_JSON_FORMAT);
                break;

            default:
                log.error("Unexpected kind of message sub type: " + subType);
                retMsg = FAIL;
                break;
        }

	retMsg.setMsgs(strBuffer.toString());

	return retMsg;
    }

    public synchronized void stopServer() {
        if (log.isTraceEnabled()) { log.trace("enter");}
        running.set(false);
        log.info("kcListener to close");
        if (log.isTraceEnabled()) { log.trace("exit");}
    }
}

package com.esgyn.kafkaCDC.server;


import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerThread;

public class KafkaCDC implements Runnable{
    private final static String DEFAULT_LOGCONFPATH = "../conf/log4j.xml";
    private final long   DEFAULT_STREAM_TO_MS = 60; // the unit is second, 60s
    private final long   DEFAULT_ZOOK_TO_MS   = 10; // the unit is second, 10s
    private final long   DEFAULT_COMMIT_COUNT = 5000;
    private final long   DEFAULT_PARALLE      = 16;
    private final long   DEFAULT_INTERVAL     = 10; // the unit is second, 10s
    private final int    DEFAULT_MAX_PARTITION= 1000;
    private final String DEFAULT_BROKER       = "localhost:9092";
    private final String DEFAULT_IPADDR       = "localhost";
    private final String DEFAULT_PORT         = "23400";
    private final String DEFAULT_SCHEMA       = "SEABASE";
    private final String DEFAULT_USER         = "db__root";
    private final String DEFAULT_PASSWORD     = "zz";

    private final String DEFAULT_ENCODING     = "UTF8";
    private final String DEFAULT_KEY          = "org.apache.kafka.common.serialization.StringDeserializer";
    private final String DEFAULT_VALUE        = "org.apache.kafka.common.serialization.StringDeserializer";

    private static Logger log = Logger.getLogger(KafkaCDC.class); 

    long     commitCount  = DEFAULT_COMMIT_COUNT;
    String   broker       = DEFAULT_BROKER;
    String   format       = null;
    String   groupID      = null;
    int []   partitions   = null;
    String   topic        = null;
    String   zookeeper    = null;

    boolean  full         = false;
    boolean  bigEndian    = false;
    boolean  skip         = false;
    boolean  keepalive    = false;
    long     streamTO     = DEFAULT_STREAM_TO_MS;
    long     zkTO         = DEFAULT_ZOOK_TO_MS;
    long     interval     = DEFAULT_INTERVAL;

    String   dbip         = DEFAULT_IPADDR;
    String   dbport       = DEFAULT_PORT;
    String   defschema    = null;
    String   deftable     = null;
    String   dburl        = null;
    String   dbdriver     = "org.trafodion.jdbc.t4.T4Driver";  
    String   delimiter    = null;
    String   dbuser       = DEFAULT_USER;
    String   dbpassword   = DEFAULT_PASSWORD;

    String   tenantUser   = null;
    String   charEncoding = DEFAULT_ENCODING;
    String   key          = DEFAULT_KEY;
    String   value        = DEFAULT_VALUE;

    private volatile int              running = 0;
    private EsgynDB                   esgyndb = null;
    private ArrayList<ConsumerThread> consumers = null;
    private class KafkaCDCCtrlCThread extends Thread 
    {
        public KafkaCDCCtrlCThread() 
	{  
            super("CtrlCThread");  
        }  

        public void run() 
	{
	    // show help or version information
	    if (consumers == null)
		return;

            log.warn("exiting via Ctrl+C!");

	    for (ConsumerThread consumer : consumers) {
		try{
		    log.info("waiting for [" + consumer.getName() + "] stop.");
		    consumer.Close();
		    consumer.join();
		    log.info(consumer.getName() + " stoped success.");
		} catch(Exception e){
		    log.error("wait " + consumer.getName() + " stoped fail!");
		    e.printStackTrace();
		} 
	    }

	    if (esgyndb != null){
		log.info("show the last results:");
		esgyndb.DisplayDatabase();
	    } else {
		log.warn("didn't connect to database!");
	    }
            running = 0;  
        }  
    }  

    public KafkaCDC() 
    {  
        Runtime.getRuntime().addShutdownHook(new KafkaCDCCtrlCThread());  
    }  

    public void run() 
    {  
	log.info("keepalive thread start to run");  
        while (running != 0) {
	    try {
		Thread.sleep(interval);
		if (keepalive && !esgyndb.KeepAlive()){
		    log.error("keepalive thread is disconnected from EsgynDB!");
                break;
		}
		esgyndb.DisplayDatabase();
		for (ConsumerThread consumer : consumers) {
		    if (!consumer.GetState()){
			running--;
		    }
		}
	    } catch (InterruptedException ie) {
		log.error("keepalive throw InterruptedException " 
			  + ie.getMessage());
		ie.printStackTrace(); 
		break;
	    } catch (Exception e) {
		log.error("keepalive throw Exception " + e.getMessage());
		e.printStackTrace(); 
		break;
	    }
        }
	log.warn("keepalive thread exited!");  
    }

    public void init( String [] args ) throws ParseException {
	/*
	 * Get command line args
	 * 
	 * Cmd line params:
	 * -b --broker <arg>   broker location (node0:9092[,node1:9092])
	 * -c --commit <arg>   num message per Kakfa synch (num recs, default is 5000)
	 * -d --dbip <arg>     database server ip
	 * -e --encode <arg>   character encoding of data, default: utf8
	 * -f,--format <arg>   format of data, default: ""
	 * -g --group <arg>    groupID
	 * -h --help           show help information
	 * -p --partition <arg>the partition number (default is 16)
	 * -s --schema <arg>   schema
	 * -t --topic <arg>    topic
	 * -v --version        print version info
	 * -z --zk <arg>       zookeeper connection (node:port[/kafka?]
	 *
	 * --dbport <arg>      database server port
	 * --dbuser <arg>      database server user
	 * --dbpw <arg>        database server password
	 * --delim <arg>       field delimiter, default: ','(comma)
	 * --bigendian         the data format is bigendian, default is little endian
	 * --full              pull data from beginning
	 * --interval <arg>    the print state time interval
	 * --key <arg>         key deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer
	 * --sto <arg>         stream T/O (default is 60000ms)
	 * --skip              skip the data error
	 * --table <arg>       table name, default: null
	 * --tenant <arg>      database tenant user
	 * --value <arg>       value  deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer
	 * --zkto <arg>        zk T/O (default is 10000ms)
	 */
	Options exeOptions = new Options();
	Option brokerOption = Option.builder("b")
	    .longOpt("broker")
	    .required(false)
	    .hasArg()
	    .desc("bootstrap.servers setting, ex: <node>:9092, default: "
		  + "\"localhost:9092\"")
	    .build();
	Option commitOption = Option.builder("c")
	    .longOpt("commit")
	    .required(false)
	    .hasArg()
	    .desc("num message per Kakfa synch, default: 5000")
	    .build();
	Option dbipOption = Option.builder("d")
	    .longOpt("dbip")
	    .required(false)
	    .hasArg()
	    .desc("database server ip, default: \"localhost\"")
	    .build();
	Option encodeOption = Option.builder("e")
	    .longOpt("encode")
	    .required(false)
	    .hasArg()
	    .desc("character encoding of data, default: \"utf8\"")
	    .build();
	Option formatOption = Option.builder("f")
	    .longOpt("format")
	    .required(false)
	    .hasArg()
	    .desc("format of data, support \"Unicom\" and \"HongQuan\" now, "
		  + "default: \"\"")
	    .build();
	Option groupOption = Option.builder("g")
	    .longOpt("group")
	    .required(false)
	    .hasArg()
	    .desc("group for this consumer, default: 0")
	    .build();
	Option helpOption = Option.builder("h")
	    .longOpt("help")
	    .required(false)
	    .desc("show help information")
	    .build();
	Option partitionOption = Option.builder("p")
	    .longOpt("partition")
	    .required(false)
	    .hasArg()
	    .desc("partition number to process message, one thread only process "
		  + " the data from one partition, default: 16. the format: "
		  + "\"id [, id] ...\", id should be: \"id-id\". example: "
		  + "\n\ta. -p \"1,4-5,8\" : means process the partition "
		  + "0,3,4,5 and 6" 
		  + "\n\tb. -p 4 : means process the partition 0,1,2 and 3" 
		  + "\n\tc. -p \"2-2\" : means process the partition 3")
	    .build();
	Option schemaOption = Option.builder("s")
	    .longOpt("schema")
	    .required(false)
	    .hasArg()
	    .desc("default database schema, use the schema from data without"
		  + " this option, default: null")
	    .build();
	Option topicOption = Option.builder("t")
	    .longOpt("topic")
	    .required(false)
	    .hasArg()
	    .desc("REQUIRED. topic of subscription")
	    .build();
	Option zkOption = Option.builder("z")
	    .longOpt("zook")
	    .required(false)
	    .hasArg()
	    .desc("zookeeper connection list, ex: <node>:port[/kafka],...")
	    .build();
	Option versionOption = Option.builder("v")
	    .longOpt("version")
	    .required(false)
	    .desc("print the version of KafkaCDC")
	    .build();
	Option dbportOption = Option.builder()
	    .longOpt("dbport")
	    .required(false)
	    .hasArg()
	    .desc("database server port, default: 23400")
	    .build();
	Option dbuserOption = Option.builder()
	    .longOpt("dbuser")
	    .required(false)
	    .hasArg()
	    .desc("database server user, default: db__root")
	    .build();
	Option dbpwOption = Option.builder()
	    .longOpt("dbpw")
	    .required(false)
	    .hasArg()
	    .desc("database server password, default: zz")
	    .build();
	Option delimOption = Option.builder()
	    .longOpt("delim")
	    .required(false)
	    .hasArg()
	    .desc("field delimiter, default: ','(comma)")
	    .build();
	Option bigendianOption = Option.builder()
	    .longOpt("bigendian")
	    .required(false)
	    .desc("the data format is big endian, default: little endian")
	    .build();
	Option fullOption = Option.builder()
	    .longOpt("full")
	    .required(false)
	    .desc("pull data from beginning, default: false")
	    .build();
	Option intervalOption = Option.builder()
	    .longOpt("interval")
	    .required(false)
	    .hasArg()
	    .desc("the print state time interval, the unit is second, default: 10s")
	    .build();
	Option keepaliveOption = Option.builder()
	    .longOpt("keepalive")
	    .required(false)
	    .hasArg()
	    .desc("check database keepalive, default is false")
	    .build();
	Option keyOption = Option.builder()
	    .longOpt("key")
	    .required(false)
	    .hasArg()
	    .desc("key deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer")
	    .build();
	Option skipOption = Option.builder()
	    .longOpt("skip")
	    .required(false)
	    .desc("skip all errors of data, default: false")
	    .build();
	Option stoOption = Option.builder()
	    .longOpt("sto")
	    .required(false)
	    .hasArg()
	    .desc("kafka poll time-out limit, default: 60s")
	    .build();
	Option tableOption = Option.builder()
	    .longOpt("table")
	    .required(false)
	    .hasArg()
	    .desc("table name, default: null,you should write like this [tablename]  if tablename is lowerCase")
	    .build();
	Option tenantOption = Option.builder()
	    .longOpt("tenant")
	    .required(false)
	    .hasArg()
	    .desc("tanent user name, default: null")
	    .build();
	Option valueOption = Option.builder()
	    .longOpt("value")
	    .required(false)
	    .hasArg()
	    .desc("value deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer")
	    .build();
	Option zktoOption = Option.builder()
	    .longOpt("zkto")
	    .required(false)
	    .hasArg()
	    .desc("zookeeper time-out limit, default: 10s")
	    .build();

	exeOptions.addOption(brokerOption);
	exeOptions.addOption(commitOption);
	exeOptions.addOption(dbipOption);
	exeOptions.addOption(encodeOption);
	exeOptions.addOption(formatOption);
	exeOptions.addOption(groupOption);
	exeOptions.addOption(helpOption);
	exeOptions.addOption(partitionOption);
	exeOptions.addOption(schemaOption);
	exeOptions.addOption(topicOption);
	exeOptions.addOption(versionOption);
	exeOptions.addOption(zkOption);

	exeOptions.addOption(dbportOption);
	exeOptions.addOption(dbuserOption);
	exeOptions.addOption(dbpwOption);

	exeOptions.addOption(delimOption);
	exeOptions.addOption(bigendianOption);
	exeOptions.addOption(fullOption);
	exeOptions.addOption(intervalOption);
	exeOptions.addOption(keyOption);
	exeOptions.addOption(keepaliveOption);
	exeOptions.addOption(skipOption);
	exeOptions.addOption(stoOption);
	exeOptions.addOption(tableOption);
	exeOptions.addOption(tenantOption);
	exeOptions.addOption(valueOption);
	exeOptions.addOption(zktoOption);
		
	// With required options, can't have HELP option to display help as it will only 
	// indicate that "required options are missing"
	if (args.length == 0) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("must with parameter -t topic");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}
	DefaultParser parser = new DefaultParser();
	CommandLine cmdLine = parser.parse(exeOptions, args);

	boolean getVersion = cmdLine.hasOption("version") ? true : false;

	if (getVersion){
	    log.info("KafkaCDC current version is KafkaCDC-R1.0.0");
	    System.exit(0);
	}

	boolean getHelp = cmdLine.hasOption("help") ? true : false;

	if (getHelp){
	    HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	// for the required options, move the value
	broker = cmdLine.hasOption("broker") ?  cmdLine.getOptionValue("broker")
	    : DEFAULT_BROKER;
	commitCount = cmdLine.hasOption("commit") ? 
	    Long.parseLong(cmdLine.getOptionValue("commit")) 
	    : DEFAULT_COMMIT_COUNT;
	dbip = cmdLine.hasOption("dbip") ? cmdLine.getOptionValue("dbip")
	    : DEFAULT_IPADDR;
	charEncoding = cmdLine.hasOption("encode") ? cmdLine.getOptionValue("encode")
	    : DEFAULT_ENCODING;
	format= cmdLine.hasOption("format") ? cmdLine.getOptionValue("format")
	    : "";
	groupID = cmdLine.hasOption("group") ? cmdLine.getOptionValue("group")
	    : "group_0";
	String partString = cmdLine.hasOption("partition") ? 
	    cmdLine.getOptionValue("partition") : "16";
	String[] parts = partString.split(",");
	ArrayList<Integer> tempParts = new ArrayList<Integer>(0);
	if (parts.length < 1) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("partition parameter format error [" + partString
		       + "], the right format: \"id [, id] ...\", "
		       + "id should be: \"id-id\"");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	String[] items = parts[0].split("-");
	if (parts.length == 1 && items.length <= 1) {
	    int partend   = Integer.parseInt(items[0]);
	    tempParts.clear();
	    for (int cur = 0; cur < partend; cur++) {
		tempParts.add(cur);
	    }	    
	} else {
	    for (String part : parts) {
		items = part.split("-");
		if (items == null || items.length > 2 || items.length == 0) {
		    HelpFormatter formatter = new HelpFormatter();
		    log.error ("partition parameter format error [" + partString
			       + "], the right format: \"id [, id] ...\", "
			       + "id should be: \"id-id\"");
		    formatter.printHelp("Consumer Server", exeOptions);
		    System.exit(0);
		} else if (items.length == 2) {
		    int partstart = Integer.parseInt(items[0]);
		    int partend   = Integer.parseInt(items[1]);
		    for (int cur = partstart; cur <= partend; cur++) {
			tempParts.add(cur);
		    }
		} else {
		    tempParts.add(Integer.parseInt(items[0]));
		}
	    }
	}

	if (tempParts.size() > DEFAULT_MAX_PARTITION) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("partition cann't more than [" + DEFAULT_MAX_PARTITION + "]");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	partitions = new int [tempParts.size()];
	int i = 0;
	for (Integer tempPart : tempParts){
	    partitions[i++] = tempPart.intValue();  
	}

	Arrays.sort(partitions);
	for (i = 1; i < partitions.length; i++) {
	    if (partitions[i-1] == partitions[i]){
		HelpFormatter formatter = new HelpFormatter();
		log.error ("partition parameter duplicate error [" + partString 
			   + "], pre: " + partitions[i-1] + ", cur: " 
			   + partitions[i] + ", total: " + partitions.length
			   + ", off: " + i);
		formatter.printHelp("Consumer Server", exeOptions);
		System.exit(0);
	    }
	}

	defschema = cmdLine.hasOption("schema") ? cmdLine.getOptionValue("schema")
	    : null;
	topic = cmdLine.hasOption("topic") ? cmdLine.getOptionValue("topic")
	    : null;
	zookeeper = cmdLine.hasOption("zook") ? cmdLine.getOptionValue("zook") 
	    : null;

	dbport = cmdLine.hasOption("dbport") ? cmdLine.getOptionValue("dbport")
	    : DEFAULT_PORT;
	dbuser = cmdLine.hasOption("dbuser") ? cmdLine.getOptionValue("dbuser")
	    : DEFAULT_USER;
	dbpassword= cmdLine.hasOption("dbpw") ? cmdLine.getOptionValue("dbpw")
	    : DEFAULT_PASSWORD;

	delimiter = cmdLine.hasOption("delim") ? cmdLine.getOptionValue("delim")
	    : null;
	full = cmdLine.hasOption("full") ? true : false;
	bigEndian = cmdLine.hasOption("bigendian") ? true : false;
	interval = cmdLine.hasOption("interval") ?
            Long.parseLong(cmdLine.getOptionValue("interval")) : DEFAULT_INTERVAL;
	keepalive = cmdLine.hasOption("keepalive") ? true : false;
	key = cmdLine.hasOption("key") ? cmdLine.getOptionValue("key")
	    : DEFAULT_KEY;
	skip= cmdLine.hasOption("skip") ? true : false;
	streamTO = cmdLine.hasOption("sto") ? 
	    Long.parseLong(cmdLine.getOptionValue("sto")) : DEFAULT_STREAM_TO_MS;
	tenantUser = cmdLine.hasOption("tenant") ? cmdLine.getOptionValue("tenant")
	    : null;
	deftable = cmdLine.hasOption("table") ? cmdLine.getOptionValue("table")
	    : null;
	value = cmdLine.hasOption("value") ? cmdLine.getOptionValue("value")
	    : DEFAULT_VALUE;
	zkTO = cmdLine.hasOption("zkto") ? 
	    Long.parseLong(cmdLine.getOptionValue("zkto")) : DEFAULT_ZOOK_TO_MS;

	interval *= 1000;
	streamTO *= 1000;
	zkTO *= 1000;
	
	if (defschema != null)
	    defschema = defschema.toUpperCase();
	if (deftable != null) {
            if (deftable.startsWith("[")&&deftable.endsWith("]")) {
                deftable = deftable.substring(1, deftable.length()-1);
                log.warn("The table name is lowercase");
            }else {
                deftable = deftable.toUpperCase();
            }
	dburl = "jdbc:t4jdbc://" + dbip + ":" + dbport + "/catelog=Trafodion;"
	    + "applicationName=KafkaCDC";
	if (tenantUser != null)
	    dburl += ";tenantName=" + tenantUser;

	if (!format.equals("Unicom") && !format.equals("HongQuan") 
	    && !format.equals("")){
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("just support \"Unicom\" and \"HongQuan\" format now. "
		       + "cur format: \"" + format + "\"");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	} else if (format.equals("HongQuan")) {
	    if (!cmdLine.hasOption("key") || !cmdLine.hasOption("value")) {
		HelpFormatter formatter = new HelpFormatter();
		log.error ("\"HongQuan\" format must need key and value parameter. ");
		formatter.printHelp("Consumer Server", exeOptions);
		System.exit(0);
	    }
	}

	if (!format.equals("Unicom") && (defschema == null || deftable == null)) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("schema and table must be specified in HongQuan and Normal format.");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	if ((format.equals("") && delimiter != null && delimiter.length() != 1)) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("the delimiter must be a single character. but it's ["
		       + delimiter + "] now");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	if (defschema == null && deftable != null) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("if table is specified, schema must be specified too.");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	// one of zook must be given
	if ( topic == null ) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("the topic parameter must be specified.");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}
		// interval ||streamTO ||zkTO can't be "0"
		if (interval <= 0) {
			log.error("the interval parameter can't less than or equal \"0\" ");
			System.exit(0);
		}
		if (streamTO <= 0) {
			log.error("the sto parameter can't less than or equal \" 0\" ");
			System.exit(0);
		}
		if (zkTO <= 0) {
			log.error("the zkTO parameter can't less than or equal \"0\" ");
			System.exit(0);
		}
		}
    }

    public static void main(String[] args) 
    {
		// log4j.xml path
		DOMConfigurator.configure(DEFAULT_LOGCONFPATH);
	KafkaCDC me = new KafkaCDC();
		
	try {
	    me.init(args);
	} catch (ParseException pe) {
	    log.error ("parse parameters error:" + pe.getMessage());
	    pe.printStackTrace();
	    System.exit(0);
	}
    	Date             starttime = new Date();
	StringBuffer     strBuffer = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	strBuffer.append("KafkaCDC start time: " + sdf.format(starttime));
	strBuffer.append("\n\tbigendian   = " + me.bigEndian);
	strBuffer.append("\n\tbroker      = " + me.broker);
	strBuffer.append("\n\tcommitCount = " + me.commitCount);
	strBuffer.append("\n\tdelimiter   = " + me.delimiter);
	strBuffer.append("\n\tformat      = " + me.format);
	strBuffer.append("\n\tencode      = " + me.charEncoding);
	strBuffer.append("\n\tgroup       = " + me.groupID);
	strBuffer.append("\n\tinterval    = " + (me.interval / 1000) + "s");
	strBuffer.append("\n\tmode        = " + me.full);
	strBuffer.append("\n\tpartitions  = " + Arrays.toString(me.partitions));
	strBuffer.append("\n\tschema      = " + me.defschema);
	strBuffer.append("\n\tskip        = " + me.skip);
	strBuffer.append("\n\ttable       = " + me.deftable);
	strBuffer.append("\n\ttopic       = " + me.topic);
	strBuffer.append("\n\tzookeeper   = " + me.zookeeper); 
	strBuffer.append("\n\tkeepalive   = " + me.keepalive); 
	strBuffer.append("\n\tkey         = " + me.key); 
	strBuffer.append("\n\tvalue       = " + me.value); 

	strBuffer.append("\n\tstreamTO    = " + (me.streamTO / 1000) + "s");
	strBuffer.append("\n\tzkTO        = " + (me.zkTO / 1000) + "s");
	strBuffer.append("\n\tdburl       = " + me.dburl);
	log.info(strBuffer.toString());
	
	me.esgyndb = new EsgynDB(me.defschema,
				 me.deftable,
				 me.dburl, 
				 me.dbdriver, 
				 me.dbuser, 
				 me.dbpassword, 
				 me.interval, 
				 me.commitCount,
				 me.format.equals("HongQuan"));
	me.consumers = new ArrayList<ConsumerThread>(0);

        for (int partition : me.partitions) {
	    // connect to kafka w/ either zook setting
	    ConsumerThread consumer = new ConsumerThread(me.esgyndb,
							 me.full,
							 me.skip,
							 me.bigEndian,
							 me.delimiter,
							 me.format,
							 me.zookeeper,
							 me.broker,
							 me.topic,
							 me.groupID,
							 me.charEncoding,
							 me.key,
							 me.value,
							 partition,
							 me.streamTO,
							 me.zkTO,
							 me.commitCount);
	    consumer.setName("ConsumerThread-" + partition);
	    me.consumers.add(consumer);
	    consumer.start();
	}
	
	me.running = me.partitions.length;
	Thread ctrltrhead = new Thread(me);
        ctrltrhead.setName("CtrlCThread");

	log.info("start up CtrlCThread");
        ctrltrhead.run();  

	for (ConsumerThread consumer : me.consumers) {
	    try{
		log.info("waiting consumer [" + consumer.getName() + "] stop");
		consumer.join();
	    } catch(Exception e){
		e.printStackTrace();
	    }
	}

	log.info("all of sub threads were stoped");

    	Date endtime = new Date();
    	log.info("exit time: " + sdf.format(endtime));
    }
}

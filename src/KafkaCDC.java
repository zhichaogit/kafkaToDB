import org.apache.commons.cli.DefaultParser;

import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class KafkaCDC implements Runnable{
    private final long   DEFAULT_STREAM_TO_MS = 60000;
    private final long   DEFAULT_ZOOK_TO_MS   = 10000;
    private final long   DEFAULT_COMMIT_COUNT = 500;
    private final long   DEFAULT_PARALLE      = 16;
    private final long   DEFAULT_INTERVAL     = 10000;
    private final String DEFAULT_BROKER       = "localhost:9092";
    private final String DEFAULT_IPADDR       = "localhost";
    private final String DEFAULT_PORT         = "23400";
    private final String DEFAULT_SCHEMA       = "SEABASE";
    private final String DEFAULT_USER         = "db__root";
    private final String DEFAULT_PASSWORD     = "zz";

    private static Logger log = Logger.getLogger(KafkaCDC.class); 

    long     commitCount  = DEFAULT_COMMIT_COUNT;
    String   broker       = DEFAULT_BROKER;
    String   format       = null;
    String   groupID      = null;
    long     parallel     = DEFAULT_PARALLE;
    String   topic        = null;
    String   zookeeper    = null;

    boolean  full         = false;
    boolean  skip         = false;
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

    private boolean                   running = true;
    private EsgynDB                   esgyndb = null;
    private ArrayList<KafkaCDCThread> threads = null;
    private class CtrlCHandler extends Thread 
    {
        public CtrlCHandler() 
	{  
            super("Exit Handler");  
        }  

        public void run() 
	{
            log.warn("Ctrl+C exit!");

	    for (KafkaCDCThread thread : threads) {
		try{
		    log.info("wait sub thread stop");
		    thread.close();
		    thread.join();
		} catch(Exception e){
		    e.printStackTrace();
		} 
	    }

	    if (esgyndb != null){
		esgyndb.DisplayDatabase();
	    } else {
		log.warn("Didn't connect to database!");
	    }
            running = false;  
        }  
    }  

    public KafkaCDC() 
    {  
        Runtime.getRuntime().addShutdownHook(new CtrlCHandler());  
    }  

    public void run() 
    {  
        while (running) {
	    try {
		Thread.sleep(interval);
		if (!esgyndb.KeepAlive()){
		    log.error("Database disconnected!");
		    break;
		}
		esgyndb.DisplayDatabase();
		if (threads.size() == 0) {
		    break;
		}
	    } catch (InterruptedException e) {
		e.printStackTrace(); 
	    }
        }  
	log.info("Exit OK");  
    }

    public void init( String [] args ) throws ParseException {
	/*
	 * Get command line args
	 * 
	 * Cmd line params:
	 * -b --broker <arg>   broker location (node0:9092[,node1:9092])
	 * -c --commit <arg>   num message per Kakfa synch (num recs, default is 500)
	 * -d --dbip <arg>     database server ip
	 * -f,--format <arg>   format of data, default: unicom
	 * -g --group <arg>    groupID
	 * -p --parallel <arg> the parallel number (default is 16)
	 * -s --schema <arg>   schema
	 * -t --topic <arg>    topic
	 * -z --zk <arg>       zookeeper connection (node:port[/kafka?]
	 *
	 * --dbport <arg>      database server port
	 * --dbuser <arg>      database server user
	 * --dbpw <arg>        database server password
	 * --delim <arg>       field delimiter, default: ','(comma)
	 * --interval <arg>    the print state time interval
	 * --full              pull data from beginning
	 * --sto <arg>         stream T/O (default is 60000ms)
	 * --skip              skip the data error
	 * --table <arg>       table name, default: null
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
	    .desc("num message per Kakfa synch, default: 500")
	    .build();
	Option dbipOption = Option.builder("d")
	    .longOpt("dbip")
	    .required(false)
	    .hasArg()
	    .desc("database server ip, default: \"localhost\"")
	    .build();
	Option formatOption = Option.builder("f")
	    .longOpt("format")
	    .required(false)
	    .hasArg()
	    .desc("format of data, default: \"unicom\"")
	    .build();
	Option groupOption = Option.builder("g")
	    .longOpt("group")
	    .required(false)
	    .hasArg()
	    .desc("group for this consumer, default: 0")
	    .build();
	Option parallelOption = Option.builder("p")
	    .longOpt("parallel")
	    .required(false)
	    .hasArg()
	    .desc("parallel thread number to process message, one thread only"
		  + " process data from one partition, default: 16")
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
	    .required(true)
	    .hasArg()
	    .desc("REQUIRED. topic of subscription")
	    .build();
	Option zkOption = Option.builder("z")
	    .longOpt("zook")
	    .required(false)
	    .hasArg()
	    .desc("zookeeper connection list, ex: <node>:port[/kafka],...")
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
	Option intervalOption = Option.builder()
	    .longOpt("interval")
	    .required(false)
	    .hasArg()
	    .desc("the print state time interval, default: 10000ms")
	    .build();
	Option fullOption = Option.builder()
	    .longOpt("full")
	    .required(false)
	    .desc("pull data from beginning, default: false")
	    .build();
	Option skipOption = Option.builder()
	    .longOpt("skip")
	    .required(false)
	    .desc("skip all error of data, default: false")
	    .build();
	Option stoOption = Option.builder()
	    .longOpt("sto")
	    .required(false)
	    .hasArg()
	    .desc("kafka poll time-out limit, default: 60000ms")
	    .build();
	Option tableOption = Option.builder()
	    .longOpt("table")
	    .required(false)
	    .hasArg()
	    .desc("table name, default: null")
	    .build();
	Option zktoOption = Option.builder()
	    .longOpt("zkto")
	    .required(false)
	    .hasArg()
	    .desc("zookeeper time-out limit, default: 10000ms")
	    .build();

	exeOptions.addOption(brokerOption);
	exeOptions.addOption(commitOption);
	exeOptions.addOption(dbipOption);
	exeOptions.addOption(formatOption);
	exeOptions.addOption(groupOption);
	exeOptions.addOption(parallelOption);
	exeOptions.addOption(schemaOption);
	exeOptions.addOption(topicOption);
	exeOptions.addOption(zkOption);

	exeOptions.addOption(dbportOption);
	exeOptions.addOption(dbuserOption);
	exeOptions.addOption(dbpwOption);

	exeOptions.addOption(delimOption);
	exeOptions.addOption(intervalOption);
	exeOptions.addOption(fullOption);
	exeOptions.addOption(skipOption);
	exeOptions.addOption(stoOption);
	exeOptions.addOption(tableOption);
	exeOptions.addOption(zktoOption);
		
	// With required options, can't have HELP option to display help as it will only 
	// indicate that "required options are missing"
	if (args.length == 0) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("Must with parameter -t topic");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}
	     
	CommandLineParser parser = new DefaultParser();
	CommandLine cmdLine = parser.parse(exeOptions, args);

	// for the required options, move the value
	broker = cmdLine.hasOption("broker") ?  cmdLine.getOptionValue("broker")
	    : DEFAULT_BROKER;
	commitCount = cmdLine.hasOption("commit") ? 
	    Long.parseLong(cmdLine.getOptionValue("commit")) 
	    : DEFAULT_COMMIT_COUNT;
	dbip = cmdLine.hasOption("dbip") ? cmdLine.getOptionValue("dbip")
	    : DEFAULT_IPADDR;
	format= cmdLine.hasOption("format") ? cmdLine.getOptionValue("format")
	    : "normal";
	groupID = cmdLine.hasOption("group") ? cmdLine.getOptionValue("group")
	    : "group_0";
	parallel = cmdLine.hasOption("parallel") ? 
	    Long.parseLong(cmdLine.getOptionValue("parallel")) 
	    : DEFAULT_PARALLE;
	defschema = cmdLine.hasOption("schema") ? cmdLine.getOptionValue("schema")
	    : null;
	topic = cmdLine.getOptionValue("topic");
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
	interval = cmdLine.hasOption("interval") ?
            Long.parseLong(cmdLine.getOptionValue("interval")) : DEFAULT_INTERVAL;
	full = cmdLine.hasOption("full") ? true : false;
	skip= cmdLine.hasOption("skip") ? true : false;
	streamTO = cmdLine.hasOption("sto") ? 
	    Long.parseLong(cmdLine.getOptionValue("sto")) : DEFAULT_STREAM_TO_MS;
	deftable = cmdLine.hasOption("table") ? cmdLine.getOptionValue("table")
	    : null;
	zkTO = cmdLine.hasOption("zkto") ? 
	    Long.parseLong(cmdLine.getOptionValue("zkto")) : DEFAULT_ZOOK_TO_MS;

	if (defschema != null)
	    defschema = defschema.toUpperCase();
	if (deftable != null)
	    deftable = deftable.toUpperCase();
	if (defschema != null)
	    dburl = "jdbc:t4jdbc://" + dbip + ":" + dbport + "/schema=" + defschema;
	else
	    dburl = "jdbc:t4jdbc://" + dbip + ":" + dbport + "/schema=" + DEFAULT_SCHEMA;

	if (!format.equals("unicom") && !format.equals("normal")){
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("Just support \"unicom\" and \"normal\" format now. cur format: \"" + format + "\"");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	if (defschema == null && deftable != null) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("If table is specified table, schema must be specified too.");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}

	// one of zook must be given
	if ( topic == null ) {
	    HelpFormatter formatter = new HelpFormatter();
	    log.error ("The topic parameter must be specified.");
	    formatter.printHelp("Consumer Server", exeOptions);
	    System.exit(0);
	}
    }

    public static void main(String[] args) 
    {
	KafkaCDC me = new KafkaCDC();
		
	try {
	    me.init(args);
	} catch (ParseException p) {
	    log.error ("parameter error " + p.getMessage());
	    System.exit(0);
	}
    	Date starttime = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	StringBuffer     strBuffer = new StringBuffer();

	strBuffer.append("KafkaCDC start time: " + sdf.format(starttime));
	strBuffer.append("\n\n\tbroker    = " + me.broker);
	strBuffer.append("\n\tcommitCount = " + me.commitCount);
	strBuffer.append("\n\tdelimiter   = " + me.groupID);
	strBuffer.append("\n\tformat      = " + me.format);
	strBuffer.append("\n\tmode        = " + me.full);
	strBuffer.append("\n\tgroup       = " + me.groupID);
	strBuffer.append("\n\tinterval    = " + me.interval);
	strBuffer.append("\n\tparallel    = " + me.parallel);
	strBuffer.append("\n\tskip        = " + me.skip);
	strBuffer.append("\n\ttable       = " + me.deftable);
	strBuffer.append("\n\ttopic       = " + me.topic);
	strBuffer.append("\n\tzookeeper   = " + me.zookeeper); 

	strBuffer.append("\n\tstreamTO    = " + me.streamTO);
	strBuffer.append("\n\tzkTO        = " + me.zkTO);
	strBuffer.append("\n\tdburl       = " + me.dburl);
	log.info(strBuffer);
			
	me.esgyndb = new EsgynDB(me.defschema,
				 me.deftable,
				 me.dburl, 
				 me.dbdriver, 
				 me.dbuser, 
				 me.dbpassword, 
				 me.commitCount);
	me.threads = new ArrayList<KafkaCDCThread>(0);

        for (int i = 0; i <me.parallel; i++) {
	    // connect to kafka w/ either zook setting
	    ConsumerServer consumer = new ConsumerServer(me.esgyndb,
							 me.full,
							 me.skip,
							 me.delimiter,
							 me.format,
							 me.zookeeper,
							 me.broker,
							 me.topic,
							 me.groupID,
							 i,
							 me.streamTO,
							 me.zkTO,
							 me.commitCount);
	    KafkaCDCThread thread = new KafkaCDCThread(consumer);

	    me.threads.add(thread);
	    thread.start();
	}

	
	Thread ctrltrhead = new Thread(me);
        ctrltrhead.setName("Ctrl C Thread");

	log.info("start up ctrl+c thread");
        ctrltrhead.run();  

	for (KafkaCDCThread thread : me.threads) {
	    try{
		log.info("wait sub thread stop");
		thread.join();
	    } catch(Exception e){
		e.printStackTrace();
	    } 
	}

	log.info("all of sub thread stop");
	me.running = false;

    	Date endtime = new Date();
    	log.info("KafkaCDC end time: " + sdf.format(endtime));
    }
}

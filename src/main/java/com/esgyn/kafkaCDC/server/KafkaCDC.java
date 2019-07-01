package com.esgyn.kafkaCDC.server;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerThread;
import com.esgyn.kafkaCDC.server.kafkaConsumer.KafkaCDCUtils;

public class KafkaCDC implements Runnable {
    private final static String       DEFAULT_LOGCONFPATH   = "conf/log4j.xml";
    private final static String       kafkaCDCVersion       = "KafkaCDC-1.0.2-release";
    // the unit is second, 60s
    private final long                DEFAULT_STREAM_TO_MS  = 60;
    // the unit is second, 10s
    private final long                DEFAULT_ZOOK_TO_MS    = 10;
    private final int                 DEFAULT_HEATBEAT_TO_MS= 10;
    private final int                 DEFAULT_SESSION_TO_MS = 30;
    private final int                 DEFAULT_REQUEST_TO_MS = 305;
    private final long                DEFAULT_COMMIT_COUNT  = 5000;
    private final long                DEFAULT_PARALLE       = 16;
    // the unit is second, 10s
    private final long                DEFAULT_INTERVAL      = 10;
    private final int                 DEFAULT_MAX_PARTITION = 1000;
    private final String              DEFAULT_BROKER        = "localhost:9092";
    private final String              DEFAULT_IPADDR        = "localhost";
    private final String              DEFAULT_PORT          = "23400";
    private final String              DEFAULT_SCHEMA        = "SEABASE";
    private final String              DEFAULT_USER          = "db__root";
    private final String              DEFAULT_PASSWORD      = "zz";
    private final String              DEFAULT_ENCODING      = "UTF8";
    private final String              DEFAULT_KEY           =
            "org.apache.kafka.common.serialization.StringDeserializer";
    private final String              DEFAULT_VALUE         =
            "org.apache.kafka.common.serialization.StringDeserializer";
    private final String              DEFAULT_MESSAGECLASS  =
            "com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage";

    private static Logger log = Logger.getLogger(KafkaCDC.class);

    long   commitCount  = DEFAULT_COMMIT_COUNT;
    boolean aconn       = false;
    String broker       = DEFAULT_BROKER;
    boolean batchUpdate = false;
    String format       = null;
    String groupID      = null;
    int[]  partitions   = null;
    String partString   = null;
    int[]  existParts   = null;
    String topic        = null;
    String zookeeper    = null;

    String  full         = null;
    boolean bigEndian   = false;
    boolean skip        = false;
    boolean keepalive   = false;
    long    streamTO    = DEFAULT_STREAM_TO_MS;
    long    zkTO        = DEFAULT_ZOOK_TO_MS;
    int     hbTO        = DEFAULT_HEATBEAT_TO_MS;
    int     seTO        = DEFAULT_SESSION_TO_MS;
    int     reqTO       = DEFAULT_REQUEST_TO_MS;
    long    interval    = DEFAULT_INTERVAL;

    String  dbip        = DEFAULT_IPADDR;
    String  dbport      = DEFAULT_PORT;
    String  defschema   = null;
    String  deftable    = null;
    boolean tablespeed  = false;
    String  dburl       = null;
    String  dbdriver    = "org.trafodion.jdbc.t4.T4Driver";
    String  delimiter   = null;
    String  dbuser      = DEFAULT_USER;
    String  dbpassword  = DEFAULT_PASSWORD;
    
    String  kafkauser   = "";
    String  kafkapw     = "";

    String  outpath     = null;
    String  tenantUser  = null;
    String  charEncoding= DEFAULT_ENCODING;
    String  key         = DEFAULT_KEY;
    String  value       = DEFAULT_VALUE;
    String  messageClass= DEFAULT_MESSAGECLASS;

    private volatile int              running    = 0;
    private EsgynDB                   esgyndb    = null;
    private ArrayList<ConsumerThread> consumers  = null;

    private class KafkaCDCCtrlCThread extends Thread {
        public KafkaCDCCtrlCThread() {
            super("CtrlCThread");
        }

        public void run() {
            // show help or version information
            if (consumers == null)
                return;

            log.warn("exiting via Ctrl+C!");

            for (ConsumerThread consumer : consumers) {
                try {
                    log.info("waiting for [" + consumer.getName() + "] stop.");
                    consumer.Close();
                    consumer.join();
                    log.info(consumer.getName() + " stoped success.");
                } catch (Exception e) {
                    log.error("wait " + consumer.getName() + " stoped fail!",e);
                }
            }

            if (esgyndb != null) {
                log.info("show the last results:");
                esgyndb.DisplayDatabase();
            } else {
                log.warn("didn't connect to database!");
            }
            running = 0;
        }
    }

    public KafkaCDC() {
        Runtime.getRuntime().addShutdownHook(new KafkaCDCCtrlCThread());
    }

    public void run() {
        log.info("keepalive thread start to run");
        boolean alreadydisconnected=false;
        while (running != 0) {
            try {
                Thread.sleep(interval);
                if (alreadydisconnected) {
                    break;
                }
                esgyndb.DisplayDatabase();
                boolean firstAliveConsumer=true;
                for (int i = 0; i < consumers.size(); i++) {
                    ConsumerThread consumer=consumers.get(i);
                    if (!consumer.GetState()) {
                        running--;
                    }else {
                        if (aconn&&firstAliveConsumer) {
                            // single conn
                            if (keepalive && !consumer.KeepAliveEveryConn()) {
                                log.error("All Thread is disconnected from EsgynDB!");
                                alreadydisconnected=true;
                                break;
                            }
                            firstAliveConsumer=false;
                        }else if(!aconn){
                            //multiple conn
                            if (keepalive && !consumer.KeepAliveEveryConn()) {
                                log.error(consumer.getName()+" Thread is disconnected from EsgynDB!"
                                        + "\n this thread will stop");
                                try {
                                    consumer.Close();
                                    consumer.join();
                                    consumers.remove(i);
                                    running--;
                                    log.info(consumer.getName() + " stoped success.");
                                } catch (Exception e) {
                                    log.error("wait " + consumer.getName() + " stoped fail!",e);
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException ie) {
                log.error("keepalive throw InterruptedException " ,ie);
                break;
            } catch (Exception e) {
                log.error("keepalive throw Exception " ,e);
                break;
            }
        }
        log.warn("keepalive thread exited!");
    }

    public void init(String[] args) throws ParseException {
        /*
         * Get command line args
         * 
         * Cmd line params: 
         *    --aconn <arg>  specify one connection for esgyndb,not need arg.
         *                  default: multiple connections
         * -b --broker <arg> broker location (node0:9092[,node1:9092]) 
         *    --batchUpdate batchUpdate means update operate will batch execute,default: one by one excute
         * -c --commit <arg> num message per Kakfa synch/pull (num recs, default is 5000) 
         * -d --dbip <arg> database server ip 
         * -e --encode <arg> character encoding of data, default: utf8 
         * -f,--format <arg> format of data, default: "" 
         * -g --group <arg> groupID 
         * -h --help show help information 
         * -o --outpath write the error kafka message to this path when there are err mess.
         *         No parameters specified: not write the err message
         *         "-o /tmp/mypath" or "--outpath /tmp/mypath" : user-defined path
         * -p --partition <arg>the partition number (default is 16) 
         * -s --schema <arg> schema 
         * -t --topic <arg> topic 
         * -v --version print version info 
         * -z --zk <arg> zookeeper connection(node:port[/kafka?]
         *    --dbport <arg> database server port 
         *    --dbuser <arg> database server user 
         *    --dbpw   <arg> database server password 
         *    --delim <arg> field delimiter, default: ','(comma) 
         *    --bigendian the data format is bigendian, default is little endian 
         *    --full    pull data from beginning or End or specify the
         *              offset, default: offset submitted last time.
         *               a. --full start : means pull the all data from the beginning(earliest)
         *               b. --full end   : means pull the data from the end(latest)
         *               c. --full 1547  : means pull the data from offset 1547
         *               d. --full "yyyy-MM-dd HH:mm:ss"  : means pull the data from this date
         *    --interval <arg> the print state time interval 
         *    --key <arg> key deserializer, default is:
         *                org.apache.kafka.common.serialization.StringDeserializer 
         *    --sto <arg> stream T/O (default is 60000ms) 
         *    --skip skip the data error 
         *    --table  <arg> table name, default: null 
         *    --tablespeed <arg>  print the tables run speed info,not need arg,default:false
         *    --tenant <arg> database tenant user 
         *    --value <arg> value deserializer, default is:
         *                org.apache.kafka.common.serialization.StringDeserializer 
         *    --zkto <arg> zk T/O (default is 10000ms)
         *    --hbto <arg>        heartbeat.interval.ms, default: 10s
         *    --seto <arg>        session.timeout.ms, default: 30s
         *    --reqto <arg>       request.timeout.ms, default: 305s
         *    --kafkauser <arg> kafka user name , default: ""
         *    --kafkapw <arg> kafka passwd , default: ""
         */
        Options exeOptions = new Options();
        Option aconnOption = Option.builder().longOpt("aconn").required(false)
                .desc("specify one connection for esgyndb,not need arg."
                        + " default: multiple connections").build();
        Option brokerOption = Option.builder("b").longOpt("broker").required(false).hasArg().desc(
                "bootstrap.servers setting, ex: <node>:9092, default: " + "\"localhost:9092\"")
                .build();
        Option batchUpdateOption = Option.builder().longOpt("batchUpdate").required(false).desc(
                "batchUpdate means update operate will batch execute,default: one by one excute ")
                .build();
        Option commitOption = Option.builder("c").longOpt("commit").required(false).hasArg()
                .desc("num message per Kakfa synch/pull, default: 5000").build();
        Option dbipOption = Option.builder("d").longOpt("dbip").required(false).hasArg()
                .desc("database server ip, default: \"localhost\"").build();
        Option encodeOption = Option.builder("e").longOpt("encode").required(false).hasArg()
                .desc("character encoding of data, default: \"utf8\"").build();
        Option formatOption = Option.builder("f").longOpt("format").required(false).hasArg()
                .desc("format of data, support \"Unicom\" \"UnicomJson\" \"HongQuan\"  \"Json\" \"Protobuf\" "
                        + "and \"user-defined\" default: \"\",")
                .build();
        Option groupOption = Option.builder("g").longOpt("group").required(false).hasArg()
                .desc("group for this consumer, default: 0").build();
        Option helpOption = Option.builder("h").longOpt("help").required(false)
                .desc("show help information").build();
        Option outpathOption = Option.builder("o").longOpt("outpath").required(false).hasArg()
                .desc("write the error kafka message to this path when there are err mess."
                        +"\n No parameters specified: not write the err message"
                        +"\n \"-o /tmp/mypath\" or \"--outpath /tmp/mypath\" : user-defined path").build();
        Option partitionOption = Option.builder("p").longOpt("partition").required(false).hasArg()
                .desc("partition number to process message, one thread only process "
                        + " the data from one partition, default: 16. the format: "
                        + "\"id [, id] ...\", id should be: \"id-id\". example: "
                        + "\n\t   -p \"-1\" :means process the all partition of this topic."
                        + "\n\ta. -p \"1,4-5,8\" : means process the partition " + "1,4,5 and 8"
                        + "\n\tb. -p 4 : means process the partition 0,1,2 and 3"
                        + "\n\tc. -p \"2-2\" : means process the partition 2")
                .build();
        Option schemaOption = Option.builder("s").longOpt("schema").required(false).hasArg()
                .desc("default database schema, use the schema from data without"
                        + " this option, you should write like this [schemaName]  if "
                        + "schemaName is lowerCase. default: null")
                .build();
        Option topicOption = Option.builder("t").longOpt("topic").required(false).hasArg()
                .desc("REQUIRED. topic of subscription").build();
        Option zkOption = Option.builder("z").longOpt("zook").required(false).hasArg()
                .desc("zookeeper connection list, ex: <node>:port[/kafka],...").build();
        Option versionOption = Option.builder("v").longOpt("version").required(false)
                .desc("print the version of KafkaCDC").build();
        Option dbportOption = Option.builder().longOpt("dbport").required(false).hasArg()
                .desc("database server port, default: 23400").build();
        Option dbuserOption = Option.builder().longOpt("dbuser").required(false).hasArg()
                .desc("database server user, default: db__root").build();
        Option dbpwOption = Option.builder().longOpt("dbpw").required(false).hasArg()
                .desc("database server password, default: zz").build();
        Option delimOption = Option.builder().longOpt("delim").required(false).hasArg()
                .desc("field delimiter, default: ','(comma)").build();
        Option bigendianOption = Option.builder().longOpt("bigendian").required(false)
                .desc("the data format is big endian, default: little endian").build();
        Option fullOption = Option.builder().longOpt("full").required(false).hasArg()
                .desc("pull data from beginning or End or specify the offset, "
                        + "default: offset submitted last time."
                        + "\n\ta. --full start : means pull the all data from the beginning(earliest)"
                        + "\n\tb. --full end   : means pull the data from the end(latest)"
                        + "\n\tc. --full 1547  : means pull the data from offset 1547 "
                        + "\n\td. --full \"yyyy-MM-dd HH:mm:ss\"  : means pull the data from this date").build();
        Option intervalOption = Option.builder().longOpt("interval").required(false).hasArg()
                .desc("the print state time interval, the unit is second, default: 10s").build();
        Option keepaliveOption = Option.builder().longOpt("keepalive").required(false).hasArg()
                .desc("check database keepalive, default is false").build();
        Option keyOption = Option.builder().longOpt("key").required(false).hasArg().desc(
                "key deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer")
                .build();
        Option skipOption = Option.builder().longOpt("skip").required(false)
                .desc("skip all errors of data, default: false").build();
        Option stoOption = Option.builder().longOpt("sto").required(false).hasArg()
                .desc("kafka poll time-out limit, default: 60s").build();
        Option tableOption = Option.builder().longOpt("table").required(false).hasArg().desc(
                "table name, default: null,you should write like this [tablename]  if tablename is lowerCase"
		+ "you should write like this tablename1,tablename2  if tablename is multi-table")
                .build();
        Option tablespeedOption = Option.builder().longOpt("tablespeed").required(false)
                .desc("print the tables run speed info,not need arg,default:false")
                .build();
        Option tenantOption = Option.builder().longOpt("tenant").required(false).hasArg()
                .desc("tanent user name, default: null").build();
        Option valueOption = Option.builder().longOpt("value").required(false).hasArg().desc(
                "value deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer")
                .build();
        Option zktoOption = Option.builder().longOpt("zkto").required(false).hasArg()
                .desc("zookeeper time-out limit, default: 10s").build();
        Option hbtoOption = Option.builder().longOpt("hbto").required(false).hasArg()
                .desc("heartbeat.interval.ms, default: 10s").build();
        Option setoOption = Option.builder().longOpt("seto").required(false).hasArg()
                .desc("session.timeout.ms, default: 30s").build();
        Option reqtoOption = Option.builder().longOpt("reqto").required(false).hasArg()
                .desc("request.timeout.ms, default: 305s").build();
        Option kafkauserOption = Option.builder().longOpt("kafkauser").required(false).hasArg()
                .desc("kafka user name , default: \"\"").build();
        Option kafkapwOption = Option.builder().longOpt("kafkapw").required(false).hasArg()
                .desc("kafka password , default: \"\"").build();

        exeOptions.addOption(aconnOption);
        exeOptions.addOption(brokerOption);
        exeOptions.addOption(batchUpdateOption);
        exeOptions.addOption(commitOption);
        exeOptions.addOption(dbipOption);
        exeOptions.addOption(encodeOption);
        exeOptions.addOption(formatOption);
        exeOptions.addOption(groupOption);
        exeOptions.addOption(helpOption);
        exeOptions.addOption(outpathOption);
        exeOptions.addOption(partitionOption);
        exeOptions.addOption(schemaOption);
        exeOptions.addOption(topicOption);
        exeOptions.addOption(versionOption);
        exeOptions.addOption(zkOption);
        exeOptions.addOption(hbtoOption);
        exeOptions.addOption(setoOption);
        exeOptions.addOption(reqtoOption);

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
        exeOptions.addOption(tablespeedOption);
        exeOptions.addOption(tenantOption);
        exeOptions.addOption(valueOption);
        exeOptions.addOption(zktoOption);
        exeOptions.addOption(kafkauserOption);
        exeOptions.addOption(kafkapwOption);

        // With required options, can't have HELP option to display help as it will only
        // indicate that "required options are missing"
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            log.error("must with parameter -t topic");
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }
        DefaultParser parser = new DefaultParser();
        CommandLine cmdLine = parser.parse(exeOptions, args);

        boolean getVersion = cmdLine.hasOption("version") ? true : false;

        if (getVersion) {
            log.info("KafkaCDC current version is "+kafkaCDCVersion);
            System.exit(0);
        }

        boolean getHelp = cmdLine.hasOption("help") ? true : false;

        if (getHelp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }

        // for the required options, move the value
        aconn = cmdLine.hasOption("aconn") ? true : false;
        broker = cmdLine.hasOption("broker") ? cmdLine.getOptionValue("broker") : DEFAULT_BROKER;
        batchUpdate = cmdLine.hasOption("batchUpdate") ? true : false;
        commitCount = cmdLine.hasOption("commit") ? Long.parseLong(cmdLine.getOptionValue("commit"))
                : DEFAULT_COMMIT_COUNT;
        dbip = cmdLine.hasOption("dbip") ? cmdLine.getOptionValue("dbip") : DEFAULT_IPADDR;
        charEncoding =
                cmdLine.hasOption("encode") ? cmdLine.getOptionValue("encode") : DEFAULT_ENCODING;
        format = cmdLine.hasOption("format") ? cmdLine.getOptionValue("format") : "";
        groupID = cmdLine.hasOption("group") ? cmdLine.getOptionValue("group") : "group_0";
        partString = cmdLine.hasOption("partition") ? cmdLine.getOptionValue("partition") : "16";
        String[] parts = partString.split(",");
        if (!partString.equals("-1")) {
            ArrayList<Integer> tempParts = new ArrayList<Integer>(0);
            if (parts.length < 1) {
                HelpFormatter formatter = new HelpFormatter();
                log.error("partition parameter format error [" + partString
                        + "], the right format: \"id [, id] ...\", " + "id should be: \"id-id\"");
                formatter.printHelp("Consumer Server", exeOptions);
                System.exit(0);
            }

            String[] items = parts[0].split("-");
            if (parts.length == 1 && items.length <= 1) {
                int partend = Integer.parseInt(items[0]);
                tempParts.clear();
                for (int cur = 0; cur < partend; cur++) {
                    tempParts.add(cur);
                }
            } else {
                for (String part : parts) {
                    items = part.split("-");
                    if (items == null || items.length > 2 || items.length == 0) {
                        HelpFormatter formatter = new HelpFormatter();
                        log.error("partition parameter format error [" + partString
                                + "], the right format: \"id [, id] ...\", "
                                + "id should be: \"id-id\"");
                        formatter.printHelp("Consumer Server", exeOptions);
                        System.exit(0);
                    } else if (items.length == 2) {
                        int partstart = Integer.parseInt(items[0]);
                        int partend = Integer.parseInt(items[1]);
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
                log.error("partition cann't more than [" + DEFAULT_MAX_PARTITION + "]");
                formatter.printHelp("Consumer Server", exeOptions);
                System.exit(0);
            }

            partitions = new int[tempParts.size()];
            int i = 0;
            for (Integer tempPart : tempParts) {
                partitions[i++] = tempPart.intValue();
            }
    
            Arrays.sort(partitions);
            for (i = 1; i < partitions.length; i++) {
                if (partitions[i - 1] == partitions[i]) {
                    HelpFormatter formatter = new HelpFormatter();
                    log.error("partition parameter duplicate error [" + partString + "], pre: "
                            + partitions[i - 1] + ", cur: " + partitions[i] + ", total: "
                            + partitions.length + ", off: " + i);
                    formatter.printHelp("Consumer Server", exeOptions);
                    System.exit(0);
                }
            }
        }

        defschema  = cmdLine.hasOption("schema") ? cmdLine.getOptionValue("schema") : null;
        topic      = cmdLine.hasOption("topic") ? cmdLine.getOptionValue("topic") : null;
        zookeeper  = cmdLine.hasOption("zook") ? cmdLine.getOptionValue("zook") : null;

        dbport     = cmdLine.hasOption("dbport") ? cmdLine.getOptionValue("dbport") : DEFAULT_PORT;
        dbuser     = cmdLine.hasOption("dbuser") ? cmdLine.getOptionValue("dbuser") : DEFAULT_USER;
        dbpassword = cmdLine.hasOption("dbpw") ? cmdLine.getOptionValue("dbpw") : DEFAULT_PASSWORD;

        delimiter = cmdLine.hasOption("delim") ? cmdLine.getOptionValue("delim") : null;
        full      = cmdLine.hasOption("full") ? cmdLine.getOptionValue("full") : "";
        bigEndian = cmdLine.hasOption("bigendian") ? true : false;
        interval  =
                cmdLine.hasOption("interval") ? Long.parseLong(cmdLine.getOptionValue("interval"))
                        : DEFAULT_INTERVAL;
        keepalive = cmdLine.hasOption("keepalive") ? true : false;
        key       = cmdLine.hasOption("key") ? cmdLine.getOptionValue("key") : DEFAULT_KEY;
        outpath   = cmdLine.hasOption("outpath") ? cmdLine.getOptionValue("outpath") : null;
        skip      = cmdLine.hasOption("skip") ? true : false;
        streamTO  = cmdLine.hasOption("sto") ? Long.parseLong(cmdLine.getOptionValue("sto"))
                : DEFAULT_STREAM_TO_MS;
        tenantUser = cmdLine.hasOption("tenant") ? cmdLine.getOptionValue("tenant") : null;
        deftable = cmdLine.hasOption("table") ? cmdLine.getOptionValue("table") : null;
        tablespeed = cmdLine.hasOption("tablespeed") ? true : false;
        value    = cmdLine.hasOption("value") ? cmdLine.getOptionValue("value") : DEFAULT_VALUE;
        zkTO     = cmdLine.hasOption("zkto") ? Long.parseLong(cmdLine.getOptionValue("zkto"))
                : DEFAULT_ZOOK_TO_MS;
        hbTO     = cmdLine.hasOption("hbto") ? Integer.parseInt(cmdLine.getOptionValue("hbto"))
                : DEFAULT_HEATBEAT_TO_MS;
        seTO     = cmdLine.hasOption("seto") ? Integer.parseInt(cmdLine.getOptionValue("seto"))
                : DEFAULT_SESSION_TO_MS;
        reqTO     = cmdLine.hasOption("reqto") ? Integer.parseInt(cmdLine.getOptionValue("reqto"))
                : DEFAULT_REQUEST_TO_MS;
        kafkauser = cmdLine.hasOption("kafkauser") ? cmdLine.getOptionValue("kafkauser"):"";
        kafkapw = cmdLine.hasOption("kafkapw") ? cmdLine.getOptionValue("kafkapw"):"";

        interval *= 1000;
        streamTO *= 1000;
        zkTO *= 1000;
        hbTO *= 1000;
        seTO *= 1000;
        reqTO *= 1000;

        if (defschema != null) {
            if (defschema.startsWith("[") && defschema.endsWith("]")) {
                defschema = defschema.substring(1, defschema.length() - 1);
                log.warn("The schema name is lowercase");
            } else {
                defschema = defschema.toUpperCase();
            }
        }
        if (deftable != null) {
            if (deftable.startsWith("[") && deftable.endsWith("]")) {
                deftable = deftable.substring(1, deftable.length() - 1);
                log.warn("The table name is lowercase");
            } else {
                deftable = deftable.toUpperCase();
            }
        }
        dburl = "jdbc:t4jdbc://" + dbip + ":" + dbport + "/catelog=Trafodion;"
                + "applicationName=KafkaCDC;connectionTimeout=0";
        if (tenantUser != null)
            dburl += ";tenantName=" + tenantUser;

        if (format.equals("HongQuan")) {
            if (!cmdLine.hasOption("key") || !cmdLine.hasOption("value")) {
                HelpFormatter formatter = new HelpFormatter();
                log.error("\"HongQuan\" format must need key and value parameter. ");
                formatter.printHelp("Consumer Server", exeOptions);
                System.exit(0);
            }
        }
        if (!format.equals("") && !format.equals("Unicom") && !format.equals("Json")
                && !format.equals("Json")) {
            messageClass =
                    "com.esgyn.kafkaCDC.server.kafkaConsumer.messageType." + format + "RowMessage";
        } else {
            if (!format.equals("Unicom") && !format.equals("Json")
                    && (defschema == null || deftable == null)) {
                HelpFormatter formatter = new HelpFormatter();
                log.error(
                        "schema and table must be specified in HongQuan or Normal or Json format.");
                formatter.printHelp("Consumer Server", exeOptions);
                System.exit(0);
            }
            if (!format.equals("")) {
                messageClass = "com.esgyn.kafkaCDC.server.kafkaConsumer.messageType." + format
                        + "RowMessage";
            }
        }
        if ((format.equals("") && delimiter != null && delimiter.length() != 1)) {
            HelpFormatter formatter = new HelpFormatter();
            log.error("the delimiter must be a single character. but it's [" + delimiter + "] now");
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }

        KafkaCDCUtils utils = new KafkaCDCUtils();
        if (!full.equals("")) {
            boolean validLong = isValidLong(full);
            full=full.toUpperCase();
            if (!validLong&&!utils.isDateStr(full)&&!full.equals("START")&&!full.equals("END")) {
              log.error("the --full must have a para: \"start\" or \"end\" or a Long Numeric types"
                      + "or date types e.g.(yyyy-MM-dd HH:mm:ss)");
              System.exit(0);
            }
        }

        if (defschema == null && deftable != null) {
            HelpFormatter formatter = new HelpFormatter();
            log.error("if table is specified, schema must be specified too.");
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }

        // one of zook must be given
        if (topic == null) {
            HelpFormatter formatter = new HelpFormatter();
            log.error("the topic parameter must be specified.");
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }
        // interval ||streamTO ||zkTO || hbTO || seTO ||reqTo can't be "0"
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
        if (hbTO <= 0) {
            log.error("the hbTO parameter can't less than or equal \"0\" ");
            System.exit(0);
        }
        if (seTO <= 0) {
            log.error("the seTO parameter can't less than or equal \"0\" ");
            System.exit(0);
        }
        if (reqTO <= 0) {
            log.error("the reqTO parameter can't less than or equal \"0\" ");
            System.exit(0);
        }
        if ((!kafkapw.equals("") && kafkauser.equals("")) || 
                (kafkapw.equals("") && !kafkauser.equals(""))) {
            log.error("check the kafkauser and kafkapw parameter pls."
                    + "They must exist or not exist at the same time ");
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        // log4j.xml path
        DOMConfigurator.configure(DEFAULT_LOGCONFPATH);
        KafkaCDC me = new KafkaCDC();

        try {
            me.init(args);
        } catch (ParseException pe) {
            log.error("parse parameters error:" + pe);
            System.exit(0);
        }
        Date starttime = new Date();
        StringBuffer strBuffer = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        strBuffer.append("KafkaCDC start time: " + sdf.format(starttime));
        strBuffer.append("\n\tbigendian   = " + me.bigEndian);
        strBuffer.append("\n\toneConnect  = " + me.aconn);
        strBuffer.append("\n\tbroker      = " + me.broker);
        strBuffer.append("\n\tbatchUpdate = " + me.batchUpdate);
        strBuffer.append("\n\tcommitCount = " + me.commitCount);
        strBuffer.append("\n\tdelimiter   = \"" + me.delimiter + "\"");
        strBuffer.append("\n\tformat      = " + me.format);
        strBuffer.append("\n\tencode      = " + me.charEncoding);
        strBuffer.append("\n\tgroup       = " + me.groupID);
        strBuffer.append("\n\tinterval    = " + (me.interval / 1000) + "s");
        strBuffer.append("\n\tmode        = " + me.full);
        strBuffer.append("\n\tpartitions  = " + me.partString);
        strBuffer.append("\n\tschema      = " + me.defschema);
        strBuffer.append("\n\tskip        = " + me.skip);
        strBuffer.append("\n\ttable       = " + me.deftable);
        strBuffer.append("\n\ttopic       = " + me.topic);
        strBuffer.append("\n\ttablespeed  = " + me.tablespeed);
        strBuffer.append("\n\tzookeeper   = " + me.zookeeper);
        strBuffer.append("\n\tkeepalive   = " + me.keepalive);
        strBuffer.append("\n\tkey         = " + me.key);
        strBuffer.append("\n\tvalue       = " + me.value);
        strBuffer.append("\n\tmessageClass= " + me.messageClass);
        strBuffer.append("\n\toutpath     = " + me.outpath);

        strBuffer.append("\n\tstreamTO    = " + (me.streamTO / 1000) + "s");
        strBuffer.append("\n\tzkTO        = " + (me.zkTO / 1000) + "s");
        strBuffer.append("\n\tbhTO        = " + (me.hbTO / 1000) + "s");
        strBuffer.append("\n\tseTO        = " + (me.seTO / 1000) + "s");
        strBuffer.append("\n\treqTO       = " + (me.reqTO / 1000) + "s");
        strBuffer.append("\n\tkafkauser   = " + me.kafkauser);
        strBuffer.append("\n\tkafkapasswd = " + me.kafkapw);
        strBuffer.append("\n\tdburl       = " + me.dburl);
        log.info("\n\t----------------------current kafkaCDC version " + kafkaCDCVersion + "-----------------------");
        log.info(strBuffer.toString());

        me.esgyndb = new EsgynDB(me.defschema, me.deftable, me.dburl, me.dbdriver, me.dbuser,
                me.dbpassword, me.interval, me.commitCount, me.format.equals("HongQuan"),me.tablespeed);
        me.consumers = new ArrayList<ConsumerThread>(0);

        //get kafka info
        me.existParts=me.getPartitionArray(me.broker,me.topic,me.kafkauser,me.kafkapw);
        if (me.partString.equals("-1")) {
            me.partitions=me.existParts;
            if (me.partitions == null) {
                log.error("the topic [" + me.topic + "] maybe not exist in the broker [" +me.broker+"]");
                System.exit(0);
            }
        }else {
           List notExistPartitions = me.getNotExistParts(me.partitions, me.existParts);
           if (notExistPartitions.size() !=0) {
            log.error("there is partitons :"+Arrays.toString(me.existParts)+"in the topic:["+me.topic+"]"
                    + ",but the partitions you specify :"+notExistPartitions + "is not exist in this topic");
            System.exit(0);
          }
        }
        log.info("\n\trelPartitions  = " + Arrays.toString(me.partitions));

        if (me.aconn) {
            log.info("create a dbconn for shard connection");
            me.esgyndb.setSharedConn(me.esgyndb.CreateConnection(false));
        }
        //start consumer theads
        for (int partition : me.partitions) {
            // connect to kafka w/ either zook setting
            ConsumerThread consumer = new ConsumerThread(me.esgyndb, me.full, me.skip, me.bigEndian,
                    me.delimiter, me.format, me.zookeeper, me.broker, me.topic, me.groupID,
                    me.charEncoding, me.key, me.value,me.kafkauser,me.kafkapw, partition,
                    me.streamTO, me.zkTO,me.hbTO,me.seTO,me.reqTO,me.commitCount, me.messageClass,
                    me.outpath, me.aconn, me.batchUpdate);
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
            try {
                log.info("waiting consumer [" + consumer.getName() + "] stop");
                consumer.join();
            } catch (Exception e) {
                log.error("consumerthread join exception",e);
            }
        }

        if (me.aconn) {
            log.info("close connection");
            me.esgyndb.CloseConnection(me.esgyndb.getSharedConn());
        }
        log.info("all of sub threads were stoped");

        Date endtime = new Date();
        log.info("exit time: " + sdf.format(endtime));
    }

    // get the partition int[]
    public int[] getPartitionArray(String brokerstr, String a_topic,String kafkauser,
            String kafkapasswd) {
        int[] partitioncount=null;

        Properties props   = new Properties();
        props.put("bootstrap.servers", brokerstr);
        props.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (!kafkauser.equals("")) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule "
                    + "required username=" + kafkauser + " password="+kafkapasswd+";");
        }

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(a_topic);
        if (partitionInfos!=null) {
            partitioncount= new int[partitionInfos.size()];
            if (partitionInfos.size()!=0) {
                for (int i = 0; i < partitionInfos.size(); i++) {
                    partitioncount[i]=partitionInfos.get(i).partition();
                }
                Arrays.sort(partitioncount);
            }
        }else {
            log.error("the topic ["+ a_topic +"] is not exist in this broker ["+brokerstr +"]");
            System.exit(0);
        }
        return partitioncount;
    }
    public static boolean isValidLong(String str){
        try{
            long _v = Long.parseLong(str);
            return true;
        }catch(NumberFormatException e){
          return false;
        }
     }
    public List getNotExistParts(int[] partsArr,int[] existPartsArr) {
        List existPartitions = new ArrayList<Integer>();
        List notExistPartitions = new ArrayList<Integer>();

        for (int i = 0; i < existPartsArr.length; i++) {
            existPartitions.add(existPartsArr[i]);
        }
        for (int i = 0; i < partsArr.length; i++) {
            if (!existPartitions.contains(partsArr[i])) {
                notExistPartitions.add(partsArr[i]);
            }
        }
        return notExistPartitions;
    }
}

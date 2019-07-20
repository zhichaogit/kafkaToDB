package com.esgyn.kafkaCDC.server.utils;

import java.util.List;
import java.util.Date;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

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

import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.KafkaParams;
import com.esgyn.kafkaCDC.server.utils.EsgynDBParams;
import com.esgyn.kafkaCDC.server.utils.KafkaCDCParams;

import lombok.Getter;
import lombok.Setter;

public class Parameters {
    @Getter
    private KafkaParams    KafkaParams    = new KafkaParams();
    @Getter
    private EsgynDBParams  DBParams       = new EsgynDBParams();
    @Getter
    private KafkaCDCParams params         = new KafkaCDCParams();

    public Parameters(String[] args_) 
    {
	this.args = args_;
    }

    private static String[] args       = null;
    private static Logger   log        = Logger.getLogger(Parameters.class);

    private HelpFormatter   formatter  = new HelpFormatter();
    private Options         exeOptions = new Options();
    private CommandLine     cmdLine    = null;
    /*
     * Get command line args
     * 
     * Cmd line params: 
     *    --aconn <arg>  specify one connection for esgyndb,not need arg.
     *                  default: multiple connections
     * -b --broker <arg> broker location (node0:9092[,node1:9092]) 
     *    --batchUpdate batchUpdate means update operate will batch execute, default: one by one excute
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
    public void init()
    {
	initOptions();

        DefaultParser parser  = new DefaultParser();
	try{
	    cmdLine = parser.parse(exeOptions, args);
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error("KafkaCDC init parameters error.");
            System.exit(0);
	}

        boolean getVersion = cmdLine.hasOption("version") ? true : false;
        if (getVersion) {
            log.info("KafkaCDC current version is: " + Constants.kafkaCDCVersion);
            System.exit(0);
        }

        boolean getHelp = cmdLine.hasOption("help") ? true : false;
        if (getHelp) {
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }

        // for database options
	setDatabaseOptions();

	// for kafka options
	setKafkaOptions();

	// for KafkaCDC options
	setKafkaCDCOptions();

	checkOptions();

	reportOptions();
    }

    private void initOptions()
    {
	Option option = null;
	for (Object[] param : Constants.CONFIG_PARAMS){
	    log.debug("[" + param[0].toString() + ", " + param[1].toString()
		      + ", " + param[2].toString() + ", " + param[3].toString()
		      + ", " + param[4].toString() + "]");
            String   opt      = (String)param[0];
            String   longOpt  = (String)param[1];
            Boolean  required = (Boolean)param[2];
            Boolean  hasArg   = (Boolean)param[3];
            String   desc     = (String)param[4];

	    if (opt.equals("")){
		if (hasArg)
		    option = Option.builder().longOpt(longOpt).required(required)
			.hasArg().desc(desc).build();
		else
		    option = Option.builder().longOpt(longOpt).required(required)
			.desc(desc).build();
	    } else {
		if (hasArg)
		    option = Option.builder(opt).longOpt(longOpt).required(required)
			.hasArg().desc(desc).build();
		else
		    option = Option.builder(opt).longOpt(longOpt).required(required)
			.desc(desc).build();
	    }
	    
	    exeOptions.addOption(option);
        }
    }

    private void setDatabaseOptions()
    {
        String  dbip        = getStringParam("dbip", Constants.DEFAULT_IPADDR);
        String  dbport      = getStringParam("dbport", Constants.DEFAULT_PORT);
        String  tenantUser  = getStringParam("tenant", null);
        String  dburl = "jdbc:t4jdbc://" + dbip + ":" + dbport + "/catelog=Trafodion;"
	    + "applicationName=KafkaCDC;connectionTimeout=0";
        if (tenantUser != null)
            dburl += ";tenantName=" + tenantUser;
	DBParams.setDBUrl(dburl);
	DBParams.setDBUser(getStringParam("dbuser", Constants.DEFAULT_USER));
        DBParams.setDBPassword(getStringParam("dbpw", Constants.DEFAULT_PASSWORD));
        DBParams.setDefSchema(getStringParam("schema", null));
        DBParams.setDefTable(getStringParam("table", null));
    }

    private void setKafkaOptions()
    {
        KafkaParams.setBroker(getStringParam("broker", Constants.DEFAULT_BROKER));
	KafkaParams.setCommitCount(getLongParam("commit", Constants.DEFAULT_COMMIT_COUNT));
        KafkaParams.setFull(getStringParam("full", null));
	KafkaParams.setGroup(getStringParam("group", "group_0"));
        KafkaParams.setTopic(getStringParam("topic", null));
	KafkaParams.setKafkaUser(getStringParam("kafkauser", null));
	KafkaParams.setKafkaPW(getStringParam("kafkapw", null));
        KafkaParams.setKey(getStringParam("key", Constants.DEFAULT_KEY));
        KafkaParams.setValue(getStringParam("value", Constants.DEFAULT_VALUE));
	KafkaParams.setStreamTO(getLongParam("sto", Constants.DEFAULT_STREAM_TO_S * 1000));
	KafkaParams.setZkTO(getLongParam("zkto", Constants.DEFAULT_ZOOK_TO_S * 1000));
        KafkaParams.setHbTO(getIntParam("hbto", Constants.DEFAULT_HEATBEAT_TO_S * 1000));
	KafkaParams.setSeTO(getIntParam("seto", Constants.DEFAULT_SESSION_TO_S * 1000));
	KafkaParams.setReqTO(getIntParam("reqto", Constants.DEFAULT_REQUEST_TO_S * 1000));
        KafkaParams.setZookeeper(getStringParam("zook", null));
    }

    private void setKafkaCDCOptions()
    {
	params.setAConn(getBoolParam("aconn", false));
	params.setBatchUpdate(getBoolParam("batchUpdate", false));
	params.setBigEndian(getBoolParam("bigendian", false));
        params.setDelimiter(getStringParam("delim", null));
        params.setEncoding(getStringParam("encode", Constants.DEFAULT_ENCODING));
        params.setFormat(getStringParam("format", ""));
        params.setInterval(getLongParam("interval", Constants.DEFAULT_INTERVAL_S * 1000));
        params.setKeepalive(getBoolParam("keepalive", false));
	params.setMessageClass("com.esgyn.kafkaCDC.server.kafkaConsumer.messageType." 
			       + params.getFormat() + "RowMessage");
	params.setPartString(getStringParam("partition", "16"));
	params.setPartitions(getPartitions(params.getPartString()));
        params.setOutPath(getStringParam("outpath", null));
	params.setSkip(getBoolParam("skip", false));
        params.setTableSpeed(getBoolParam("tablespeed", false));
    }

    void checkOptions()
    {
	String defSchema = DBParams.getDefSchema();
        if (defSchema != null) {
            if (defSchema.startsWith("[") && defSchema.endsWith("]")) {
                defSchema = defSchema.substring(1, defSchema.length() - 1);
                log.warn("The schema name is lowercase");
            } else {
                defSchema = defSchema.toUpperCase();
            }
        }

	String defTable = DBParams.getDefTable();
        if (defTable != null) {
            if (defTable.startsWith("[") && defTable.endsWith("]")) {
                defTable = defTable.substring(1, defTable.length() - 1);
                log.warn("The table name is lowercase");
            } else {
                defTable = defTable.toUpperCase();
            }
        }

	String format = params.getFormat();
        if (format.equals("HongQuan")
	    && (KafkaParams.getKey() == null || KafkaParams.getValue() == null)) {
	    reportErrorAndExit("\"HongQuan\" format must need key and value parameter. ");
        }

	String messageClass = null;
        if (format.equals("") || format.equals("Unicom") || format.equals("Json")
	    || format.equals("Json")) {
            if (!format.equals("Unicom") && !format.equals("Json") 
		&& (defSchema == null || defTable == null)) {
                reportErrorAndExit("schema and table must be specified in HongQuan "
				   + "or Normal or Json format.");
            }
        }

	String delimiter = params.getDelimiter();
        if (format.equals("") && delimiter != null && delimiter.length() != 1) {
	    reportErrorAndExit("the delimiter must be a single character. but it's [" 
			       + delimiter + "] now");
        }

        Utils utils = new Utils();
	String full = KafkaParams.getFull();
        if (!full.equals("")) {
            boolean validLong = isValidLong(full);
            full = full.toUpperCase();
            if (!validLong && !utils.isDateStr(full) && !full.equals("START") && !full.equals("END")) {
		reportErrorAndExit("the --full must have a para: \"start\" or \"end\" or "
				   + "a Long Numeric types or date types e.g.(yyyy-MM-dd HH:mm:ss)");
            }
        }

        if (defSchema == null && defTable != null) {
            reportErrorAndExit("if table is specified, schema must be specified too.");
        }

        // interval ||streamTO ||zkTO || hbTO || seTO ||reqTo can't be "0"
        if (params.getInterval() <= 0) {
            reportErrorAndExit("the interval parameter can't less than or equal \"0\" ");
        }

        if (KafkaParams.getStreamTO() <= 0) {
	    reportErrorAndExit("the sto parameter can't less than or equal \" 0\" ");
        }

        if (KafkaParams.getZkTO() <= 0) {
	    reportErrorAndExit("the zkTO parameter can't less than or equal \"0\" ");
        }

        if (KafkaParams.getHbTO() <= 0) {
	    reportErrorAndExit("the hbTO parameter can't less than or equal \"0\" ");
        }

        if (KafkaParams.getSeTO() <= 0) {
	    reportErrorAndExit("the seTO parameter can't less than or equal \"0\" ");
        }

        if (KafkaParams.getReqTO() <= 0) {
	    reportErrorAndExit("the reqTO parameter can't less than or equal \"0\" ");
        }
	
	String kafkaPW = KafkaParams.getKafkaPW();
	String kafkaUser = KafkaParams.getKafkaUser();
        if ((kafkaPW != null && kafkaUser == null) || 
	    (kafkaPW == null && kafkaUser != null)) {
            reportErrorAndExit("check the kafkaUser and kafkaPW parameter pls."
			       + "They must exist or not exist at the same time ");
        }
    }

    void reportOptions()
    {
        Date starttime = new Date();
        StringBuffer strBuffer = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        strBuffer.append("\n\t----------------------current kafkaCDC version " 
			 + Constants.kafkaCDCVersion + "-----------------------\n");
        strBuffer.append("KafkaCDC start time: " + sdf.format(starttime))
	    .append("\nDatabase options:")
	    .append("\n\tdburl       = "    + DBParams.getDBUrl())
	    .append("\n\tschema      = "    + DBParams.getDefSchema())
	    .append("\n\ttable       = "    + DBParams.getDefTable())

	    .append("\n\nKafka options:")
	    .append("\n\tbroker      = "    + KafkaParams.getBroker())
	    .append("\n\tcommitCount = "    + KafkaParams.getCommitCount())
	    .append("\n\tmode        = "    + KafkaParams.getFull())
	    .append("\n\tgroup       = "    + KafkaParams.getGroup())
	    .append("\n\ttopic       = "    + KafkaParams.getTopic())
	    .append("\n\tkafkauser   = "    + KafkaParams.getKafkaUser())
	    .append("\n\tkafkapasswd = "    + KafkaParams.getKafkaPW())
	    .append("\n\tkey         = "    + KafkaParams.getKey())
	    .append("\n\tvalue       = "    + KafkaParams.getValue())
	    .append("\n\tstreamTO    = "    + KafkaParams.getStreamTO()/1000 + "s")
	    .append("\n\tzkTO        = "    + KafkaParams.getZkTO()/1000 + "s")
	    .append("\n\tbhTO        = "    + KafkaParams.getHbTO()/1000 + "s")
	    .append("\n\tseTO        = "    + KafkaParams.getSeTO()/1000 + "s")
	    .append("\n\treqTO       = "    + KafkaParams.getReqTO()/1000 + "s")
	    .append("\n\tzookeeper   = "    + KafkaParams.getZookeeper())

	    .append("\n\nKafkaCDC options:")
	    .append("\n\toneConnect  = "    + params.isAConn())
	    .append("\n\tbatchUpdate = "    + params.isBatchUpdate())
	    .append("\n\tbigendian   = "    + params.isBigEndian())
	    .append("\n\tdelimiter   = \""  + params.getDelimiter() + "\"")
	    .append("\n\tencode      = "    + params.getEncoding())
	    .append("\n\tformat      = "    + params.getFormat())
	    .append("\n\tinterval    = "    + params.getInterval()/1000 + "s")
	    .append("\n\tkeepalive   = "    + params.isKeepalive())
	    .append("\n\tmessageClass= "    + params.getMessageClass())
	    .append("\n\toutpath     = "    + params.getOutPath())
	    .append("\n\tpartString  = "    + params.getPartString())

	    .append("\n\tskip        = "    + params.isSkip())
	    .append("\n\ttablespeed  = "    + params.isTableSpeed());
        log.info(strBuffer.toString());

        // get kafka info
	int[] partitions = params.getPartitions();
        int[] existParts = getPartitionArray(KafkaParams.getBroker(), KafkaParams.getTopic(),
					     KafkaParams.getKafkaUser(), KafkaParams.getKafkaPW());
        if (params.getPartString().equals("-1")) {
            if (existParts == null) {
		reportErrorAndExit("the topic [" + KafkaParams.getTopic() 
				   + "] maybe not exist in the broker [" 
				   + KafkaParams.getBroker() + "]");
            }
            partitions = existParts;
        } else {
	    List notExistPartitions = getNotExistParts(partitions, existParts);
	    if (notExistPartitions.size() != 0) {
		reportErrorAndExit("there is partitons :" + Arrays.toString(existParts) 
				   + "in the topic:[" + KafkaParams.getTopic() 
				   + "], but the partitions you specify [" + notExistPartitions 
				   + "] is not exist in this topic");
	    }
        }
        log.info("\n\tpartitions  = " + Arrays.toString(partitions));
    }

    int[] getPartitions(String partString)
    {
	int[]    partitions = null;
        String[] parts      = partString.split(",");

        if (partString.equals("-1"))
	    return partitions;

	if (parts.length < 1) {
	    reportErrorAndExit("partition parameter format error [" + partString
			       + "], the right format: \"id [, id] ...\", " 
			       + "id should be: \"id-id\"");
	}

	ArrayList<Integer> tempParts = new ArrayList<Integer>(0);
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
		    reportErrorAndExit("partition parameter format error [" + partString
				       + "], the right format: \"id [, id] ...\", "
				       + "id should be: \"id-id\"");
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
    
	if (tempParts.size() > Constants.DEFAULT_MAX_PARTITION) {
	    reportErrorAndExit("partition cann't more than [" + Constants.DEFAULT_MAX_PARTITION + "]");
	}

	partitions = new int[tempParts.size()];
	int i = 0;
	for (Integer tempPart : tempParts) {
	    partitions[i++] = tempPart.intValue();
	}
    
	Arrays.sort(partitions);
	for (i = 1; i < partitions.length; i++) {
	    if (partitions[i - 1] == partitions[i]) {
		reportErrorAndExit("partition parameter duplicate error [" + partString + "], pre: "
				   + partitions[i - 1] + ", cur: " + partitions[i] + ", total: "
				   + partitions.length + ", off: " + i);
	    }
	}

	return partitions;
    }

    // get the partition int[]
    public int[] getPartitionArray(String brokerstr, String a_topic,String kafkaUser,
            String kafkaPW) {
        int[] partitioncount=null;

        Properties props   = new Properties();
        props.put("bootstrap.servers", brokerstr);
        props.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (kafkaUser != null && kafkaPW != null) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule "
		      + "required username=" + kafkaUser + " password=" + kafkaPW + ";");
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
	    reportErrorAndExit("the topic ["+ a_topic +"] is not exist in this broker ["+brokerstr +"]");
        }
        return partitioncount;
    }

    boolean isValidLong(String str){
        try{
            long _v = Long.parseLong(str);
            return true;
        }catch(NumberFormatException e){
          return false;
        }
    }
    
    List getNotExistParts(int[] partsArr,int[] existPartsArr) {
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

    String getStringParam(String paramName, String defStrValue)
    {
	String  param = cmdLine.hasOption(paramName) ?
	    cmdLine.getOptionValue(paramName) : defStrValue;

	return param;
    }

    boolean getBoolParam(String paramName, boolean defBoolValue)
    {
	boolean  param = cmdLine.hasOption(paramName) ? true : defBoolValue;

	return param;
    }

    long getLongParam(String paramName, long defLongValue)
    {
	long  param = cmdLine.hasOption(paramName) ?
	    Long.parseLong(cmdLine.getOptionValue(paramName)) : defLongValue;

	return param;
    }

    int getIntParam(String paramName, int defIntValue)
    {
	int  param = cmdLine.hasOption(paramName) ?
	    Integer.parseInt(cmdLine.getOptionValue(paramName)) : defIntValue;

	return param;
    }

    void reportErrorAndExit(String errorMsg)
    {
	log.error(errorMsg);
	formatter.printHelp("KafkaCDC", exeOptions);
	System.exit(0);
    }
}

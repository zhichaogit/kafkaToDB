package com.esgyn.kafkaCDC.server.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import lombok.Getter;
import lombok.Setter;

public class Parameters {
    @Setter
    @Getter
    private KafkaParams     kafka       = null;
    @Setter
    @Getter
    private DatabaseParams  database    = null;
    @Setter
    @Getter
    private KafkaCDCParams  kafkaCDC    = null;
    @Setter
    @Getter 
    private List<TableInfo> mappings    = null;
    @Getter 
    private String          startTime   = null;
    public Parameters() {};
    public Parameters(String[] args_) 
    {
	startTime = Utils.getCurrentTime();
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
     *    --aconn <arg>  specify one connection for database,not need arg.
     *                  default: multiple connections
     * -b --broker <arg> broker location (node0:9092[,node1:9092]) 
     *    --batchSize rows in one batch operator, default: 5000
     *    --batchUpdate update operate will use batch, default: false
     *    --conf <arg> read parameters from config file
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
     *    --mode    pull data from beginning or End or specify the
     *              offset, default: offset submitted last time.
     *               a. --mode start : means pull the all data from the beginning(earliest)
     *               b. --mode end   : means pull the data from the end(latest)
     *               c. --mode 1547  : means pull the data from offset 1547
     *               d. --mode "yyyy-MM-dd HH:mm:ss"  : means pull the data from this date
     *    --interval <arg> the print state time interval 
     *    --key <arg> key deserializer, default is:
     *                org.apache.kafka.common.serialization.StringDeserializer 
     *    --showConsumers     show the consumers details information, defaults:true
     *    --showLoader        show the loader details information, defaults:true
     *    --showTasks         show the consume thread tasks details information, defaults:false
     *    --showTables        show the table details information, defaults:true
     *    --showSpeed         print the tables run speed info,not need arg,default:false
     *    --sto <arg> stream T/O (default is 60000ms) 
     *    --skip skip the data error 
     *    --table  <arg> table name, default: null 
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
	    System.err.println("KafkaCDC init parameters error."+ e);
            System.exit(0);
	}

	init_log4j_conf();

        boolean getVersion = cmdLine.hasOption("version") ? true : false;
        if (getVersion) {
            log.info("KafkaCDC current version is: " + Constants.KafkaCDC_VERSION);
            System.exit(0);
        }

        boolean getHelp = cmdLine.hasOption("help") ? true : false;
        if (getHelp) {
            formatter.printHelp("Consumer Server", exeOptions);
            System.exit(0);
        }

        if (cmdLine.hasOption("conf")) {
	    Parameters params = null;
	    String confPath = getStringParam("conf", Constants.DEFAULT_JSONCONFPATH);

            try {
		params = FileUtils.jsonParse(confPath);
            } catch (Exception e) {
                log.error("parse jsonConf has an error.make sure your json file is right.", e);
                System.exit(0);
            }

	    kafka    = params.getKafka();
	    database = params.getDatabase();
	    kafkaCDC = params.getKafkaCDC();
	    mappings = params.getMappings();
        } else if (cmdLine.hasOption("encryptPW")) {
            String encryptPW = getStringParam("encryptPW", null);
            String encodePW = Utils.getEncodePW(encryptPW);
            log.info("encodePW:" + encodePW);
            System.exit(0);
        }else {

	    // for database options
	    setDatabaseOptions();

	    // for kafka options
	    setKafkaOptions();

	    // for KafkaCDC options
	    setKafkaCDCOptions();
	}

	// standardization
	reinit();

	checkOptions();

	reportOptions();

	initDirectories();
    }

    public void init_log4j_conf() {
        long logDelay = cmdLine.hasOption("logDelay") ? Long.parseLong(
                cmdLine.getOptionValue("logDelay")) : Constants.DEFAULT_LOGDELAY_TO_S;
        logDelay = logDelay*1000;
        // load configure log4j.xml
        DOMConfigurator.configureAndWatch(Constants.DEFAULT_LOGCONFPATH, logDelay);
    }

    public void reinit() {
	kafka.init();
	kafkaCDC.init(startTime);
	database.init(this);

	getTopicsPartitions();
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
	database = new DatabaseParams();

	database.setBatchSize(getLongParam("batchSize", Constants.DEFAULT_BATCH_SIZE));
	database.setBatchUpdate(getBoolParam("batchUpdate", false));
	database.setDBIP(getStringParam("dbip", Constants.DEFAULT_IPADDR));
	database.setDBPort(getStringParam("dbport", Constants.DEFAULT_PORT));
	database.setDBType(getStringParam("type", Constants.DEFAULT_DATABASE));
	database.setDBDriver(getStringParam("driver", Constants.DEFAULT_DRIVER));
	database.setDBUser(getStringParam("dbuser", Constants.DEFAULT_USER));
        database.setDBPW(getStringParam("dbpw", Constants.DEFAULT_PASSWORD));
	database.setDBTenant(getStringParam("tenant", null));
        database.setDefSchema(getStringParam("schema", null));
        database.setDefTable(getStringParam("table", null));
        database.setKeepalive(getBoolParam("keepalive", false));
    }

    private void setKafkaOptions()
    {
	kafka = new KafkaParams();
        kafka.setBroker(getStringParam("broker", Constants.DEFAULT_BROKER));
	kafka.setFetchSize(getLongParam("fetchSize", Constants.DEFAULT_FETCH_SIZE));
	String mode = getStringParam("mode", null);
        kafka.setMode(mode.toUpperCase());

	// handle topics
	TopicParams topic = new TopicParams();
	topic.setTopic(getStringParam("topic", null));
	topic.setPartition(getStringParam("partition", "16"));
	topic.setGroup(getStringParam("group", "group_0"));
	List<TopicParams> topics = new ArrayList<TopicParams>();
	topics.add(topic);
	kafka.setTopics(topics);

	kafka.setKafkaUser(getStringParam("kafkauser", null));
	kafka.setKafkaPW(getStringParam("kafkapw", null));
        kafka.setKey(getStringParam("key", Constants.DEFAULT_KEY));
        kafka.setValue(getStringParam("value", Constants.DEFAULT_VALUE));
	kafka.setStreamTO(getLongParam("sto", Constants.DEFAULT_STREAM_TO_S));
	kafka.setWaitTO(getLongParam("wto", Constants.DEFAULT_WAIT_TO_S));
	kafka.setZkTO(getLongParam("zkto", Constants.DEFAULT_ZOOK_TO_S));
        kafka.setHbTO(getIntParam("hbto", Constants.DEFAULT_HEATBEAT_TO_S));
	kafka.setSeTO(getIntParam("seto", Constants.DEFAULT_SESSION_TO_S));
	kafka.setReqTO(getIntParam("reqto", Constants.DEFAULT_REQUEST_TO_S));
        kafka.setZookeeper(getStringParam("zook", null));
    }

    private void setKafkaCDCOptions()
    {
	kafkaCDC = new KafkaCDCParams();
	kafkaCDC.setConsumers(getLongParam("consumers", Constants.DEFAULT_CONSUMERS));
	kafkaCDC.setBigEndian(getBoolParam("bigendian", false));
        kafkaCDC.setDelimiter(getStringParam("delim", Constants.DEFAULT_DELIMITER));
	kafkaCDC.setDumpBinary(getBoolParam("dumpbinary", false));
        kafkaCDC.setEncoding(getStringParam("encode", Constants.DEFAULT_ENCODING));
        kafkaCDC.setFormat(getStringParam("format", ""));
        kafkaCDC.setInterval(getLongParam("interval", Constants.DEFAULT_INTERVAL_S));
	kafkaCDC.setSkip(getBoolParam("skip", false));
	kafkaCDC.setLoaders(getLongParam("loader", Constants.DEFAULT_LOADERS));
	kafkaCDC.setLoadDir(getStringParam("loadDir", "load"));
	kafkaCDC.setKafkaDir(getStringParam("kafkaDir", "kafka"));
	kafkaCDC.setShowConsumers(getBoolParam("showConsumers", true));
	kafkaCDC.setShowLoaders(getBoolParam("showLoaders", true));
	kafkaCDC.setShowTasks(getBoolParam("showTasks", false));
	kafkaCDC.setShowTables(getBoolParam("showTables", true));
	kafkaCDC.setShowSpeed(getBoolParam("showSpeed", false));
    }

    private void checkOptions()
    {
	String defSchema = database.getDefSchema();
	String defTable = database.getDefTable();

	String format = kafkaCDC.getFormat();
        if (format.equals("HongQuan")
	    && (kafka.getKey() == null || kafka.getValue() == null)) {
	    reportErrorAndExit("\"HongQuan\" format must need key and value parameter. ");
        }

	String messageClass = null;
        if (format.equals("") || format.equals("Unicom") || format.equals("Json")) {
            if (!format.equals("Unicom") && !format.equals("Json") 
		&& (defSchema == null || defTable == null)) {
                reportErrorAndExit("schema and table must be specified in HongQuan "
				   + "or Normal or Json format.");
            }
        }

	String delimiter = kafkaCDC.getDelimiter();
        if (format.equals("") && delimiter != null && delimiter.length() != 1) {
	    reportErrorAndExit("the delimiter must be a single character. but it's [" 
			       + delimiter + "] now");
        }

	String mode = kafka.getMode();
        if (!mode.equals("")) {
            boolean validLong = isValidLong(mode);
            if (!validLong && !Utils.isDateStr(mode) && !mode.equals("START") 
		&& !mode.equals("END")) {
		reportErrorAndExit("the --mode must have a para: \"start\" or \"end\" or "
				   + "a Long Numeric types or date types e.g.(yyyy-MM-dd HH:mm:ss)");
            }
        }

        if (defSchema == null && defTable != null) {
            reportErrorAndExit("if table is specified, schema must be specified too.");
        }

        // interval ||zkTO || hbTO || seTO ||reqTo can't be "0"
        if (kafkaCDC.getInterval() <= 0) {
            reportErrorAndExit("the interval parameter can't less than or equal \"0\" ");
        }

        if (kafka.getZkTO() <= 0) {
	    reportErrorAndExit("the zkTO parameter can't less than or equal \"0\" ");
        }

        if (kafka.getHbTO() <= 0) {
	    reportErrorAndExit("the hbTO parameter can't less than or equal \"0\" ");
        }

        if (kafka.getSeTO() <= 0) {
	    reportErrorAndExit("the seTO parameter can't less than or equal \"0\" ");
        }

        if (kafka.getReqTO() <= 0) {
	    reportErrorAndExit("the reqTO parameter can't less than or equal \"0\" ");
        }
	
	String kafkaPW   = kafka.getKafkaPW();
	String kafkaUser = kafka.getKafkaUser();
        if ((kafkaPW != null && kafkaUser == null) || 
	    (kafkaPW == null && kafkaUser != null)) {
            reportErrorAndExit("check the kafkaUser and kafkaPW parameter pls."
			       + "They must exist or not exist at the same time ");
        }
    }

    private void reportOptions()
    {
        StringBuffer strBuffer = new StringBuffer();

        strBuffer.append("\n\n----------------------current kafkaCDC version " 
			 + Constants.KafkaCDC_VERSION + "-----------------------\n");
        strBuffer.append("KafkaCDC start time: " + startTime)
	    .append(database.toString())
	    .append(kafka.toString())
	    .append(kafkaCDC.toString());

        log.info(strBuffer.toString());
    }

    private void initDirectories()
    {
	FileUtils.createDirectory(Constants.DEFAULT_LOG_PATH);
	FileUtils.createDirectory(Constants.DEFAULT_LOG_PATH + startTime);
	FileUtils.createDirectory(kafkaCDC.getKafkaDir());
	FileUtils.createDirectory(kafkaCDC.getLoadDir());
    }

    private void getTopicsPartitions()
    {
	List<TopicParams> topics = kafka.getTopics();

        Properties props       = new Properties();
	String     kafkaUser   = kafka.getKafkaUser();
	String     kafkaPW     = kafka.getKafkaPW();

	props.put("bootstrap.servers", kafka.getBroker());
        props.put("key.deserializer", Constants.KEY_STRING);
        props.put("value.deserializer", Constants.VALUE_STRING);
        if (kafkaUser != null && kafkaPW != null) {
            kafkaPW = Utils.getDecodePW(kafkaPW);
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config", Constants.SEC_PLAIN_STRING
		      + kafkaUser + " password=" + kafkaPW + ";");
        }

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);

	if (topics == null) {
	    Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();

	    topics = new ArrayList<TopicParams>();

	    for (Map.Entry<String, List<PartitionInfo>> entry : topicMap.entrySet()) {
		TopicParams topicParams = new TopicParams();
		String      topicName   = entry.getKey();
		int         partitionID = -1;

		topicParams.setTopic(topicName);
		List<PartitionInfo> partitionInfos = entry.getValue();

		if (partitionInfos != null) {
		    partitionID = partitionInfos.size();
		    topicParams.setPartition(String.valueOf(partitionID));
		}else {
		    reportErrorAndExit("the topic [" + topicName + 
				       "] is not exist in this broker [" 
				       + kafka.getBroker() + "]");
		}

		topicParams.setPartitions(getPartsArrayFromKafka(consumer, topicName));

		if (log.isDebugEnabled()) {
		    log.debug("Topic [" + topicName + "], partition [ " + partitionID + "]");
		}
	    }
	}

	for(TopicParams topic : topics){
	    String topicName  = topic.getTopic();
	    int [] partitions = topic.getPartitions();
	    
	    if (partitions == null){
		int [] kafkaParts = getPartsArrayFromKafka(consumer, topicName);
		partitions = getPartArrayFromStr(topic.getPartition());
		if (partitions==null) {
		    partitions=kafkaParts;
		}
		checkKafkaPartitions(topicName, partitions, kafkaParts);

		topic.setPartitions(partitions);
	    }
	}
    }

    // cann't input -1, only can input "1,5"
    private int[] getPartArrayFromStr(String partition) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	int[]    partitions = null;

	if (partition.equals("-1")){
	    return partitions;
	}

        String[] parts      = partition.split(",");
	if (parts.length < 1) {
	    reportErrorAndExit("partition parameter format error [" + partition
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
		    reportErrorAndExit("partition parameter format error [" + partition
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
		reportErrorAndExit("partition parameter duplicate error [" + partition + "], pre: "
				   + partitions[i - 1] + ", cur: " + partitions[i] + ", total: "
				   + partitions.length + ", off: " + i);
	    }
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return partitions;
    }

    public List getNotExistParts(int[] partsArr, int[] existPartsArr) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

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

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return notExistPartitions;
    }

    public void checkKafkaPartitions(String topic, int [] partitions, int [] existParts){
        if (log.isTraceEnabled()) { log.trace("enter"); }
        if (log.isDebugEnabled()) {
            log.debug("topic[" + topic + "],specify parts[" + Arrays.toString(partitions)
            + "],existParts[" + Arrays.toString(existParts)+"]");
        }

	List notExistPartitions = getNotExistParts(partitions, existParts);
	if (notExistPartitions.size() != 0) {
	    reportErrorAndExit("there is partitons [" + Arrays.toString(existParts)
			       + "] in the topic [" + topic + "], but the partitions you specify ["
			       + notExistPartitions + "] is not exist in this topic");
	}
        log.info("\n\ttopic [" + topic + "] partitions  = " + Arrays.toString(partitions));

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    // get the partition int[]
    private int[] getPartsArrayFromKafka(KafkaConsumer<byte[], byte[]> consumer, String topic) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        int[] partitions = null;

	List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
	if (partitionInfos != null) {
	    partitions= new int[partitionInfos.size()];
	    if (partitionInfos.size()!=0) {
		for (int i = 0; i < partitionInfos.size(); i++) {
		    partitions[i] = partitionInfos.get(i).partition();
		}
		Arrays.sort(partitions);
	    }
	}else {
	    reportErrorAndExit("the topic [" + topic + "] is not exist in this broker ["
			       + kafka.getBroker() + "]");
	}

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return partitions;
    }

    public boolean isValidLong(String str){
        try{
            long _v = Long.parseLong(str);
            return true;
        }catch(NumberFormatException e){
          return false;
        }
    }
    
    private String getStringParam(String paramName, String defStrValue)
    {
	String  param = cmdLine.hasOption(paramName) ?
	    cmdLine.getOptionValue(paramName) : defStrValue;

	return param;
    }

    private boolean getBoolParam(String paramName, boolean defBoolValue)
    {
	boolean  param = cmdLine.hasOption(paramName) ? true : defBoolValue;

	return param;
    }

    private long getLongParam(String paramName, long defLongValue)
    {
	long  param = cmdLine.hasOption(paramName) ?
	    Long.parseLong(cmdLine.getOptionValue(paramName)) : defLongValue;

	return param;
    }

    private int getIntParam(String paramName, int defIntValue)
    {
	int  param = cmdLine.hasOption(paramName) ?
	    Integer.parseInt(cmdLine.getOptionValue(paramName)) : defIntValue;

	return param;
    }

    private void reportErrorAndExit(String errorMsg)
    {
	log.error(errorMsg);
	formatter.printHelp("KafkaCDC", exeOptions);
	System.exit(0);
    }
}

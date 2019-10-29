package com.esgyn.kafkaCDC.server.utils;

public class Constants {
    public final static String       DEFAULT_LOGCONFPATH   = "conf/log4j.xml";
    public final static String       DEFAULT_JSONCONFPATH  = "conf/kafkaCDC.json";
    public final static String       DEFAULT_LOG_PATH      = "logs/";
    public final static String       DEFAULT_UNLOAD_PATH   = "unload/";
    public final static String       KafkaCDC_VERSION      = "KafkaCDC-2.0.0";

    // the unit is second
    public final static long         DEFAULT_LOGDELAY_TO_S = 10;
    public final static long         DEFAULT_STREAM_TO_S   = -1;
    public final static long         DEFAULT_WAIT_TO_S     = 1;
    public final static long         DEFAULT_ZOOK_TO_S     = 10;
    public final static int          DEFAULT_HEATBEAT_TO_S = 10;
    public final static int          DEFAULT_SESSION_TO_S  = 30;
    public final static int          DEFAULT_REQUEST_TO_S  = 305;
    public final static long         DEFAULT_CLEANDELAY_S  = 3600;
    public final static long         DEFAULT_INTERVAL_S    = 10;
    public final static long         DEFAULT_CLEAN_I_S     = 60;

    public final static long         DEFAULT_BATCH_SIZE    = 5000;
    public final static long         DEFAULT_FETCH_SIZE    = 10000;
    public final static long         DEFAULT_PARALLE       = 16;
    public final static int          DEFAULT_MAX_PARTITION = 1000;
    public final static int          DEFAULT_FETCH_BYTES   = 104857600;
    public final static String       DEFAULT_DELIMITER     = "\\,";

    // database information
    public final static String       DEFAULT_DATABASE      = "EsgynDB";
    public final static String       DEFAULT_DRIVER        = "org.trafodion.jdbc.t4.T4Driver";
    public final static String       DEFAULT_IPADDR        = "localhost";
    public final static String       DEFAULT_PORT          = "23400";
    public final static String       DEFAULT_SCHEMA        = "SEABASE";
    public final static String       DEFAULT_USER          = "db__root";
    public final static String       DEFAULT_PASSWORD      = "zz";
    public final static String       DEFAULT_ENCODING      = "UTF8";
    public final static long         DEFAULT_COMMITSIZE    = 512*1024*1024;

    // Kafka information
    public final static String       DEFAULT_BROKER        = "localhost:9092";
    public final static long         DEFAULT_LOADERS       = 4;
    public final static long         DEFAULT_MAXWAITTASKS  = 2;
    public final static long         DEFAULT_CONSUMERS     = 4;
    public final static String       DEFAULT_KEY           =
	"org.apache.kafka.common.serialization.StringDeserializer";
    public final static String       DEFAULT_VALUE         =
	"org.apache.kafka.common.serialization.StringDeserializer";
    public final static String       DEFAULT_MESSAGECLASS  =
	"com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage";

    // const value
    public final static String       KEY_STRING            =
	"org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public final static String       VALUE_STRING          =
	"org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public final static String       SEC_PLAIN_STRING      =
	"org.apache.kafka.common.security.plain.PlainLoginModule required username=";

    public final static Object[][]   CONFIG_PARAMS = {
	// database information
        {"",   "batchSize",   false,  true, "batch means update operate will batch execute,"
	 + " default: 5000 "},
	{"",   "batchUpdate", false, false, "update operate will use batch, default: true"},
	{"",   "conf",        false,  true, "specified configuration parameter file"},
	{"",   "cleanDelayTime",   false,  true, "clean the log delay time,default: 3600s"},
	{"",   "cleanInterval",false,  true, "clean log interval time,-1 will not clean.default: 10s"},
        {"d",  "dbip",        false,  true, "database server ip, default: \"localhost\""},
        {"",   "dbport",      false,  true, "database server port, default: 23400"},
        {"",   "dbuser",      false,  true, "database server user, default: db__root"},
        {"",   "dbpw",        false,  true, "database server password, default: org.trafodion.jdbc.t4.T4Driver"},
        {"",   "driver",      false,  true, "database driver, default: "},
        {"s",  "schema",      false,  true, "default database schema, use the schema from data without this option, you should write like this [schemaName]  if schemaName is lowerCase. default: null"},
        {"",   "table",       false,  true, "table name, default: null, you should write like this [tablename]  if tablename is lowerCase you should write like this tablename1,tablename2 "
	 + "if tablename is multi-table"},
        {"",   "tenant",      false,  true, "tanent user name, default: null"},

	// kafka information
        {"b",  "broker",      false,  true, "bootstrap.servers setting, ex: <node>:9092, default: \"localhost:9092\""},
        {"",   "fetchSize",   false,  true, "num message per Kakfa synch/pull, default: 10000"},
        {"",   "mode",        false,  true, "pull data from beginning or End or specify the offset, default: offset submitted last time.\n"
	 + "\ta. --mode start : means pull the all data from the beginning(earliest)\n"
	 + "\tb. --mode end   : means pull the data from the end(latest)\n"
	 + "\tc. --mode 1547  : means pull the data from offset 1547 \n"
	 + "\td. --mode \"yyyy-MM-dd HH:mm:ss\"  : means pull the data from this date"},
        {"f",  "fetchBytes",  false,  true, "fetch data from kafka size, default: 10485760, it's 100MB"},
        {"g",  "group",       false,  true, "group for this consumer, default: 0"},
        {"p",  "partition",   false,  true, "partition number to process message, one thread only process the data from one partition, default: 16. the format: \"id [, id] ...\", id should be: \"id-id\". "
	 + "example: \n"
	 + "\ta. -p \"1,4-5,8\" : means process the partition 1,4,5 and 8\n"
	 + "\tb. -p 4           : means process the partition 0,1,2 and 3\n"
	 + "\tc. -p \"2-2\"     : means process the partition 2\n"
	 + "\td. -p \"-1\"      : means process the all partition of this topic."},
        {"t",  "topic",       false,  true, "REQUIRED. topic of subscription"},
	{"",   "kafkauser",   false,  true, "kafka user name , default: \"\""},
	{"",   "kafkapw",     false,  true, "kafka password , default: \"\""},
        {"",   "key",         false,  true, "key deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer"},
        {"",   "value",       false,  true, "value deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer"},
        {"",   "sto",         false,  true, "consumer free TimeOut,-1 not TO forever, default: -1"},
        {"",   "zkto",        false,  true, "zookeeper time-out limit, default: 10s"},
        {"",   "hbto",        false,  true, "heartbeat.interval.ms, default: 10s"},
        {"",   "seto",        false,  true, "session.timeout.ms, default: 30s"},
        {"",   "reqto",       false,  true, "request.timeout.ms, default: 305s"},
        {"z",  "zook",        false,  true, "zookeeper connection list, ex: <node>:port[/kafka],..."},

	// KafkaCDC information
        {"",   "consumers",   false,  true, "specify connection number to kafka, default: 4."},
        {"",   "bigendian",   false, false, "the data format is big endian, default: little endian"},
        {"",   "delim",       false,  true, "field delimiter, default: ','(comma)"},
        {"e",  "encode",      false,  true, "character encoding of data, default: \"utf8\""},
        {"",   "encryptPW",   false,  true, "encryption the password"},
        {"f",  "format",      false,  true, "format of data, support \"Unicom\" \"UnicomJson\" \"HongQuan\"  \"Json\" \"Protobuf\" and \"user-defined\" default: \"\","},
        {"",   "interval",    false,  true, "the print state time interval, the unit is second, default: 10s"},
        {"",   "keepalive",   false,  true, "check database keepalive, default is false"},
        {"",   "skip",        false, false, "skip all errors of data, default: false"},
	{"",   "loader",      false,  true, "processer number, default:4"},
	{"",   "maxWaitTasks",false,  true, "max wait Tasks size, default:2"},
	{"",   "logDelay",    false,  true, "Dynamically load log4j.xml conf files interval time, default:10s"},
	{"",   "showConsumers",false, false, "show the consumer thread details, default: true"},
	{"",   "showLoaders", false, false, "show the loader thread details, default: true"},
	{"",   "showTasks",   false, false, "show the consumers task details, default: false"},
	{"",   "showTables",  false, false, "show the tables details, default: true"},
	{"",   "showSpeed",   false, false, "print the tables run speed info, not need arg, default:false"},
	{"",   "loaddir",     false,  true, "dump process data file path,default: null"},
	{"",   "kafkadir",    false,  true, "dump consumer data file path,default: null"},

	// system info
        {"h",  "help",        false, false, "show help information"},
        {"v",  "version",     false, false, "print the version of KafkaCDC"}
    };   
}

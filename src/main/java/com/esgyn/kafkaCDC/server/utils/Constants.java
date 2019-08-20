package com.esgyn.kafkaCDC.server.utils;

public class Constants {
    public final static String       DEFAULT_LOGCONFPATH   = "conf/log4j.xml";
    public final static String       DEFAULT_JSONCONFPATH  = "conf/kafkaCDC.json";
    public final static String       KafkaCDC_VERSION      = "KafkaCDC-1.0.2-release";

    // the unit is second
    public final static long         DEFAULT_STREAM_TO_S   = 60;
    public final static long         DEFAULT_ZOOK_TO_S     = 10;
    public final static int          DEFAULT_HEATBEAT_TO_S = 10;
    public final static int          DEFAULT_SESSION_TO_S  = 30;
    public final static int          DEFAULT_REQUEST_TO_S  = 305;

    public final static long         DEFAULT_COMMIT_COUNT  = 5000;
    public final static long         DEFAULT_PARALLE       = 16;

    // the unit is second, 10s
    public final static long         DEFAULT_INTERVAL_S    = 10;
    public final static int          DEFAULT_MAX_PARTITION = 1000;

    // database information
    public final static String       DEFAULT_DRIVER        = "org.trafodion.jdbc.t4.T4Driver";
    public final static String       DEFAULT_IPADDR        = "localhost";
    public final static String       DEFAULT_PORT          = "23400";
    public final static String       DEFAULT_SCHEMA        = "SEABASE";
    public final static String       DEFAULT_USER          = "db__root";
    public final static String       DEFAULT_PASSWORD      = "zz";
    public final static String       DEFAULT_ENCODING      = "UTF8";

    // Kafka information
    public final static String       DEFAULT_BROKER        = "localhost:9092";
    public final static String       DEFAULT_KEY           =
	"org.apache.kafka.common.serialization.StringDeserializer";
    public final static String       DEFAULT_VALUE         =
	"org.apache.kafka.common.serialization.StringDeserializer";
    public final static String       DEFAULT_MESSAGECLASS  =
	"com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage";

    public final static Object[][]   CONFIG_PARAMS = {
	// database information
        {"",   "batchUpdate", false, false, "batchUpdate means update operate will batch execute,"
	 + " default: one by one excute "},
	{"",   "conf",        false,  true, "database server ip, default: \"localhost\""},
        {"d",  "dbip",        false,  true, "database server ip, default: \"localhost\""},
        {"",   "dbport",      false,  true, "database server port, default: 23400"},
        {"",   "dbuser",      false,  true, "database server user, default: db__root"},
        {"",   "dbpw",        false,  true, "database server password, default: zz"},
        {"s",  "schema",      false,  true, "default database schema, " 
	 + "use the schema from data without this option, "
	 + "you should write like this [schemaName]  if schemaName is lowerCase. default: null"},
        {"",   "table",       false,  true, "table name, default: null,"
	 + " you should write like this [tablename]  if tablename is lowerCase "
	 + "you should write like this tablename1,tablename2  if tablename is multi-table"},
        {"",   "tenant",      false,  true, "tanent user name, default: null"},

	// kafka information
        {"b",  "broker",      false,  true, "bootstrap.servers setting, ex: <node>:9092, default: " 
	 + "\"localhost:9092\""},
        {"c",  "commit",      false,  true, "num message per Kakfa synch/pull, default: 5000"},
        {"",   "full",        false,  true, "pull data from beginning or End or specify the offset, "
	 + "default: offset submitted last time.\n"
	 + "\ta. --full start : means pull the all data from the beginning(earliest)\n"
	 + "\tb. --full end   : means pull the data from the end(latest)\n"
	 + "\tc. --full 1547  : means pull the data from offset 1547 \n"
	 + "\td. --full \"yyyy-MM-dd HH:mm:ss\"  : means pull the data from this date"},
        {"g",  "group",       false,  true, "group for this consumer, default: 0"},
        {"p",  "partition",   false,  true, "partition number to process message, " 
	 + "one thread only process the data from one partition, default: 16. "
	 + "the format: \"id [, id] ...\", id should be: \"id-id\". "
	 + "example: \n\t   -p \"-1\" :means process the all partition of this topic.\n"
	 + "\ta. -p \"1,4-5,8\" : means process the partition " + "1,4,5 and 8\n"
	 + "\tb. -p 4 : means process the partition 0,1,2 and 3\n"
	 + "\tc. -p \"2-2\" : means process the partition 2"},
        {"t",  "topic",       false,  true, "REQUIRED. topic of subscription"},
	{"",   "kafkauser",   false,  true, "kafka user name , default: \"\""},
	{"",   "kafkapw",     false,  true, "kafka password , default: \"\""},
        {"",   "key",         false,  true, "key deserializer, "
	 + "default is: org.apache.kafka.common.serialization.StringDeserializer"},
        {"",   "value",       false,  true, "value deserializer, " 
	 + "default is: org.apache.kafka.common.serialization.StringDeserializer"},
        {"",   "sto",         false,  true, "kafka poll time-out limit, default: 60s"},
        {"",   "zkto",        false,  true, "zookeeper time-out limit, default: 10s"},
        {"",   "hbto",        false,  true, "heartbeat.interval.ms, default: 10s"},
        {"",   "seto",        false,  true, "session.timeout.ms, default: 30s"},
        {"",   "reqto",       false,  true, "request.timeout.ms, default: 305s"},
        {"z",  "zook",        false,  true, "zookeeper connection list, ex: <node>:port[/kafka],..."},

	// KafkaCDC information
        {"",   "aconn",       false, false, "specify one connection for esgyndb,not need arg. "
	 + "default: multiple connections"},
        {"",   "bigendian",   false, false, "the data format is big endian, default: little endian"},
        {"",   "delim",       false,  true, "field delimiter, default: ','(comma)"},
        {"e",  "encode",      false,  true, "character encoding of data, default: \"utf8\""},
        {"f",  "format",      false,  true, "format of data, support \"Unicom\" \"UnicomJson\""
	 + " \"HongQuan\"  \"Json\" \"Protobuf\" and \"user-defined\" default: \"\","},
        {"",   "interval",    false,  true, "the print state time interval, the unit is second, "
	 + "default: 10s"},
        {"",   "keepalive",   false,  true, "check database keepalive, default is false"},
        {"o",  "outpath",     false,  true, "write the error kafka message to this path"
	 + " when there are err mess.\n No parameters specified: not write the err message\n"
	 + " \"-o /tmp/mypath\" or \"--outpath /tmp/mypath\" : user-defined path"},
        {"",   "skip",        false, false, "skip all errors of data, default: false"},
	{"",   "tablespeed",  false, false, "print the tables run speed info, not need arg, "
	 + "default:false"},

	// system info
        {"h",  "help",        false, false, "show help information"},
        {"v",  "version",     false, false, "print the version of KafkaCDC"}
    };   
}

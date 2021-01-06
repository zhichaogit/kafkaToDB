# Build Process:
KafkaToDB depends jdbc jar files(reference pom.xml):

jdbcT4-2.4.7.jar couldn't download it by maven ,so you should move it to 
/home/${user}/.m2/repository/org/apache/trafodion/jdbc/t4/jdbcT4/2.4.7
 by yourself.

# KafkaToDB
usage: Consumer Server
* -b,--broker <arg>     bootstrap.servers setting, ex: <node>:9092,
                        default: "localhost:9092"
*    --batchSize <arg>  batch means update operate will batch execute,
                        default: 500
*    --batchUpdate      update operate will use batch, default: false
*    --ignore           ignore the data where user defined, default: false
*    --bigendian        the data format is big endian, default: little endian
*    --cleanInterval <arg> clean log interval time,-1 will not clean.default: 10s
*    --cleanDelayTime <arg>  clean the log delay time,default: 3600s
*    --conf <arg>       specified configuration parameter file
*    --consumers <arg>  specify connection number to kafka, default: 4.
* -d,--dbip <arg>       database server ip, default: "localhost"
*    --dbport <arg>     database server port, default: 23400
*    --dbpw <arg>       database server password, default:
                        org.trafodion.jdbc.t4.T4Driver
*    --dbuser <arg>     database server user, default: db__root
*    --delim <arg>      field delimiter, default: ','(comma)
*    --driver <arg>     database driver, default:
* -e,--encode <arg>     character encoding of data, default: "utf8"
*    --encryptPW <arg>  encryption the password
* -f,--format <arg>     format of data, support "Unicom" "UnicomJson"
                        "HongQuan"  "Json" "Protobuf" and "user-defined"
                        default: "",
*    --fetchSize <arg>  num message per Kakfa synch/pull, default: 1000
* -g,--group <arg>      group for this consumer, default: 0
* -h,--help             show help information
*   --hbto <arg>        heartbeat.interval.ms, default: 10s
*    --interval <arg>   the print state time interval, the unit is second,
                        default: 10s
*    --kafkadir <arg>   dump consumer data file path,default:null
*    --kafkapw <arg>    kafka password , default: ""
*    --kafkauser <arg>  kafka user name , default: ""
*    --keepalive <arg>  check database keepalive, default is false
*    --key <arg>        key deserializer, default is:
                        org.apache.kafka.common.serialization.StringDeserializer
*    --loaddir <arg>    dump process data file path,default:null
*    --loader <arg>     processer number, default:4
*    --maxWaitTasks<arg> max wait Tasks size, default:2
*    --maxBackupIndex <arg>   maxBackupIndex for dump to file,-1 is the
                             Integer.MAX_VALUE. default:10
*    --maxFileSize <arg> maxFileSize for dump to file, default:1024MB
*    --networkTO <arg>  Sets a time limit that the driver waits for a reply, default:30
*    --logDelay <arg>   Dynamically load log4j.xml conf files interval
                        time, default:10s
*    --mode <arg>       pull data from beginning or End or specify the
                        offset, default: offset submitted last time.
                        a. --mode start : means pull the all data from the
                        beginning(earliest)
                        b. --mode end   : means pull the data from the end(latest)
                        c. --mode 1547  : means pull the data from offset 1547
                        d. --mode "yyyy-MM-dd HH:mm:ss"  : means pull the
                        data from this date
* -p,--partition <arg>  partition number to process message, one thread
                        only process the data from one partition, default:
                      16. the format: "id [, id] ...", id should be:"id-id". 
                      example:
                         -p "-1" :means process the all partition of this topic.
                      a. -p "1,4-5,8" : means process the partition 1,4,5 and 8
                      b. -p  4 : means process the partition 0,1,2 and 3
                      c. -p "2-2" : means process the partition 2
*    --port <arg>        kafkaCDC server listener port,default:8889
*    --reqto <arg>      request.timeout.ms, default: 305s
* -s,--schema <arg>     default database schema, use the schema from data
                        without this option, you should write like this
                        [schemaName]  if schemaName is lowerCase. default:
                        null
*    --seto <arg>       session.timeout.ms, default: 30s
*    --showConsumers    show the consumer thread details, default: true
*    --showLoaders      show the loader thread details, default: true
*    --showSpeed        print the tables run speed info, not need arg,
                        default:false
*    --showTables       show the tables details, default: false
*    --showTasks        show the consumers task details, default: true
*    --skip             skip all errors of data, default: false
*    --sto <arg>        consumer free TimeOut,-1 not TO forever, default: -1
* -t,--topic <arg>      REQUIRED. topic of subscription
*    --table <arg>      table name, default: null, you should write like
                        this [tablename]  if tablename is lowerCase you
                        should write like this tablename1,tablename2 if
                        tablename is multi-table
*    --tenant <arg>     tanent user name, default: null
* -v,--version          print the version of KafkaCDC
*    --value <arg>      value deserializer, default is:
                        org.apache.kafka.common.serialization.StringDeserializer
* -z,--zook <arg>       zookeeper connection list, ex:<node>:port[/kafka],...
*    --zkto <arg>       zookeeper time-out limit, default: 10s



# example:
Must create the schema and tables first of all.
Must have maven and JDK.

# normal
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --mode start -dbuser trafodion --dbpw traf123
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --mode start --sto 20 --interval 10 --sto 20  --dbuser trafodion --dbpw traf123 --fetchSize 500 --batchSize 500 -delim "|"
# somecom
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t g_ad --mode start --dbuser trafodion --dbpw traf123 -f HongQuan -s kafkaCDC --table hqTable  --sto 20 --interval 10 --zkto 20 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# com2
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --mode start --dbuser trafodion --dbpw traf123 -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --mode start --dbuser trafodion --dbpw traf123 -s SEABASE  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --mode start --dbuser trafodion --dbpw traf123 -s SEABASE --table tab -t test --sto 20 --interval 10 --zkto 20 --dbip localhost --fetchSize 500 --batchSize 500

# Json
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Json --mode start --dbuser trafodion --dbpw traf123 -s [schemaname] -t testTopic --sto 20 --interval 10 --fetchSize 500 --batchSize 500

# Protobuf
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f Protobuf --mode start --dbuser trafodion --dbpw traf123 -s schemaname -t testTopic -f  --encode GBK --sto 20 --interval 5 --fetchSize 500 --batchSize 500 --key org.apache.kafka.common.serialization.ByteArrayDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f Protobuf --mode start --dbuser trafodion --dbpw traf123 -s schemaname -t testTopic -f  --encode GBK --sto 20 --interval 5 --fetchSize 500 --batchSize 500 --key org.apache.kafka.common.serialization.ByteArrayDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer  --kafkauser username --kafkapw password

# comJson And authentication
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f UnicomJson --mode start --dbuser trafodion --dbpw traf123 -s schemaName  -t testTopic --sto 20 --interval 10  --fetchSize 500 --batchSize 500  --kafkauser username --kafkapw passwd

#.encryption the password(e.g. encodePW:[dHJhZjEyMw==],the [dHJhZjEyMw==] is your password)
*./encryptPW.sh password

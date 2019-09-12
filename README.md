# Build Process:
KafkaCDC depends some jar files(reference pom.xml):
1. commons-cli-1.4.jar
2. jackson-annotations-2.9.0.jar
3. jackson-core-2.9.0.jar
4. jackson-databind-2.9.0.jar
5. jdbcT4-2.4.7.jar
6. jline-0.9.94.jar
7. jopt-simple-5.0.4.jar
8. kafka_2.11-1.1.0.jar
9. kafka-clients-1.1.0.jar
10. log4j-1.2.17.jar
11. lz4-java-1.4.jar
12. metrics-core-2.2.0.jar
13. netty-3.10.5.Final.jar
14. scala-library-2.11.12.jar
15. scala-logging_2.11-3.7.2.jar
16. scala-reflect-2.11.12.jar
17. slf4j-api-1.7.25.jar
18. slf4j-log4j12-1.7.25.jar
19. snappy-java-1.1.7.1.jar
20. zkclient-0.10.jar
21. zookeeper-3.4.10.jar
22. protobuf-java-3.6.0.jar
23. commons-lang3-3.4.jar

jdbcT4-2.4.7.jar couldn't download it by maven ,so you should move it to 
/home/${user}/.m2/repository/org/apache/trafodion/jdbc/t4/jdbcT4/2.4.7
 by yourself.

# KafkaCDC
usage: Consumer Server
*    --aconn <arg>       specify one connection for database,not need arg.default: multiple connections
* -b,--broker <arg>      bootstrap.servers setting, ex: <node>:9092,default: "localhost:9092"
*    --bigendian         the data format is big endian, default: little endian
*    --batchUpdate       batchUpdate means update operate will batch execute,default: one by one excute
* -c,--commit <arg>      num message per Kakfa synch/pull, default: 5000
* -d,--dbip <arg>        database server ip, default: "localhost"
*    --dbport <arg>      database server port, default: 23400
*    --dbpw <arg>        database server password, default: zz
*    --dbuser <arg>      database server user, default: db__root
*    --delim <arg>       field delimiter, default: ','(comma)
* -e,--encode <arg>      character encoding of data, default: "utf8"
* -f,--format <arg>      format of data, support "Unicom" "UnicomJson" "HongQuan"  "Json" "Protobuf" now, default: ""
*    --full              pull data from beginning or End or specify the<br>
                         offset, default: offset submitted last time.<br>
                         a. --full start : means pull the all data from the beginning(earliest)<br>
                         b. --full end   : means pull the data from the end(latest)<br>
                         c. --full 1547  : means pull the data from offset 1547<br>
                         d. --full "yyyy-MM-dd HH:mm:ss"  : means pull the data from this date<br>
* -g,--group <arg>       group for this consumer, default: 0
* -h,--help              show help information
*    --hbto <arg>        heartbeat.interval.ms, default: 10s
*    --interval <arg>    the print state time interval, the unit is second,default: 10s
*    --keepalive <arg>   check database keepalive, default is false
*    --key <arg>         key deserializer, default is:<br>
                         org.apache.kafka.common.serialization.StringDeserializer
*    --kafkauser <arg>   kafka user name , default: ""
*    --kafkapw   <arg>   kafka password , default: ""
* -p,--partition <arg>   partition number to process message, one thread only process<br>
                         the data from one partition,default: 16. the format: "id [, id] ...", id <br>
                         should be: "id-id". example:<br>
                            -p "-1" :means process the all partition of this topic.<br>
                         a. -p "1,4-5,8" : means process the partition 1,4,5 and 8<br>
                         b. -p 4 : means process the partition 0,1,2 and 3 <br>
                         c. -p "2-2" : means process the partition 2<br>
*    --reqto <arg>       request.timeout.ms, default: 305s
* -s,--schema <arg>      default database schema, use the schema from data without this option, <br>
                         you should write like this [schemaName] if schemaName is lowerCase. default:null<>
*    --seto <arg>        session.timeout.ms, default: 30s
*    --showConsumers     show the consumers details information, defaults:true
*    --showLoader        show the loader details information, defaults:true
*    --showTasks         show the consume thread tasks details information, defaults:false
*    --showTables        show the table details information, defaults:true
*    --showSpeed         print the tables run speed info,not need arg,default:false
*    --skip              skip all errors of data, default: false
*    --sto <arg>         kafka poll time-out limit, default: 60s
* -t,--topic <arg>       REQUIRED. topic of subscription
*    --table <arg>       table name, default: null,you should write like this [tablename]  if tablename is lowerCase
*    --tenant <arg>      tanent user name, default: null
* -v,--version           print the version of KafkaCDC
*    --value <arg>       value deserializer, default is:<br>
                         org.apache.kafka.common.serialization.StringDeserializer
* -z,--zook <arg>        zookeeper connection list, ex:<node>:port[/kafka],...
*    --zkto <arg>        zookeeper time-out limit, default: 10s

# KafkaCDC Dataflow
* the dataflow of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/new_design/design/dataflow.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/dataflow.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

# KafkaCDC Design
* the architecture of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/master/new_design/architecture.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/architecture.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

* the classes of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/new_design/design/classes.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/classes.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

# example:
Must create the schema and tables first of all.
Must have maven and JDK.

# normal
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --full start -dbuser trafodion --dbpw traf123
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --full start --sto 20 --interval 10 --sto 20  --dbuser trafodion --dbpw traf123 -c 500 -delim "|"
# HongQuan
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t g_ad --full start --dbuser trafodion --dbpw traf123 -f HongQuan -s kafkaCDC --table hqTable  --sto 20 --interval 10 --zkto 20 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# Unicom
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full start --dbuser trafodion --dbpw traf123 -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full start --dbuser trafodion --dbpw traf123 -s SEABASE  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full start --dbuser trafodion --dbpw traf123 -s SEABASE --table tab -t test --sto 20 --interval 10 --zkto 20 --dbip localhost -c 500

# Json
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Json --full start --dbuser trafodion --dbpw traf123 -s [schemaname] -t testTopic --sto 20 --interval 10 -c 500

# Protobuf
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f Protobuf --full start --dbuser trafodion --dbpw traf123 -s schemaname -t testTopic -f  --encode GBK --sto 20 --interval 5 -c 50 --key org.apache.kafka.common.serialization.ByteArrayDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f Protobuf --full start --dbuser trafodion --dbpw traf123 -s schemaname -t testTopic -f  --encode GBK --sto 20 --interval 5 -c 50 --key org.apache.kafka.common.serialization.ByteArrayDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer  --kafkauser username --kafkapw password

# UnicomJson And authentication
*./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost  -g 1 -f UnicomJson --full start --dbuser trafodion --dbpw traf123 -s schemaName  -t testTopic --sto 20 --interval 10  -c 500  --kafkauser username --kafkapw passwd

# Build Process:
KafkaCDC depends some jar files(reference pom.xml):
1. commons-cli-1.4.jar
2. jackson-annotations-2.9.0.jar
3. jackson-core-2.9.0.jar
4. jackson-databind-2.9.0.jar
5. jdbcT4-2.3.302UTT.jar
6. jline-0.9.94.jar
7. jopt-simple-5.0.4.jar
8. kafka_2.11-1.1.0.jar
9. kafka-clients-1.1.0.jar
10. log4j-1.2.17.jar
11. lz4-java-1.4.jar
12. metrics-core-2.2.0.jar
13. netty-3.10.5.Final.jar
14. scala-library-2.11.12.jar
15. cala-logging_2.11-3.7.2.jar
16. scala-reflect-2.11.12.jar
17. slf4j-api-1.7.25.jar
18. slf4j-log4j12-1.7.25.jar
19. snappy-java-1.1.7.1.jar
20. zkclient-0.10.jar
21. zookeeper-3.4.10.jar

jdbcT4-2.2.0.jar have some modify.so you should move it to 
/home/${user}/repository/org/apache/trafodion/jdbc/t4/jdbcT4/2.3.302UTT
 by yourself.

# KafkaCDC
usage: Consumer Server

* -b,--broker <arg>      bootstrap.servers setting, ex: <node>:9092,default: "localhost:9092"
*    --bigendian         the data format is big endian, default: little endian
* -c,--commit <arg>      num message per Kakfa synch, default: 5000
* -d,--dbip <arg>        database server ip, default: "localhost"
*    --dbport <arg>      database server port, default: 23400
*    --dbpw <arg>        database server password, default: zz
*    --dbuser <arg>      database server user, default: db__root
*    --delim <arg>       field delimiter, default: ','(comma)
* -e,--encode <arg>      character encoding of data, default: "utf8"
* -f,--format <arg>      format of data, support "Unicom"  "HongQuan"  "Json" and "normal" now, default(normal): ""
*    --full              pull data from beginning, default: false
* -g,--group <arg>       group for this consumer, default: 0
* -h,--help              show help information
*    --interval <arg>    the print state time interval, the unit is second,default: 10s
*    --keepalive <arg>   check database keepalive, default is false
*    --key <arg>         key deserializer, default is:<br>
                         org.apache.kafka.common.serialization.StringDeserializer
* -p,--partition <arg>   partition number to process message, one thread only process<br>
                         the data from one partition,default: 16. the format: "id [, id] ...", id
                         should be: "id-id". example:
                         a. -p "1,4-5,8" : means process the partition 0,3,4,5 and 6
                         b. -p 4 : means process the partition 0,1,2 and 3 
                         c. -p "2-2" : means process the partition 3
* -s,--schema <arg>     default database schema, use the schema from data without this option, <br>
                        you should write like this [schemaName] if schemaName is lowerCase. default:null
*    --skip             skip all errors of data, default: false
*    --sto <arg>        kafka poll time-out limit, default: 60s
* -t,--topic <arg>      REQUIRED. topic of subscription
*    --table <arg>      table name, default: null,you should write like this [tablename]  if tablename is lowerCase
*    --tenant <arg>     tanent user name, default: null
* -v,--version          print the version of KafkaCDC
*    --value <arg>      value deserializer, default is:<br>
                        org.apache.kafka.common.serialization.StringDeserializer
* -z,--zook <arg>        zookeeper connection list, ex:<node>:port[/kafka],...
*    --zkto <arg>        zookeeper time-out limit, default: 10s

# KafkaCDC Design
* the architecture of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/master/design/architecture.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/architecture.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

* the classes of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/master/design/classes.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/classes.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

# example:
Must create the schema and tables first of all.
Must have maven and JDK.

# normal
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --full --dbuser trafodion --dbpw traf123
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t test --full --sto 20 --interval 10 --sto 20  --dbuser trafodion --dbpw traf123 -c 500 -delim "|"
# HongQuan
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -s SEABASE --table tab -t g_ad --full --dbuser trafodion --dbpw traf123 -f HongQuan -s kafkaCDC --table hqTable  --sto 20 --interval 10 --zkto 20 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# Unicom
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full --dbuser trafodion --dbpw traf123 -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full --dbuser trafodion --dbpw traf123 -s SEABASE  -t test
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Unicom --full --dbuser trafodion --dbpw traf123 -s SEABASE --table tab -t test --sto 20 --interval 10 --zkto 20 --dbip localhost -c 500

#Json
* ./KafkaCDC-server.sh -p 1 -b localhost:9092 -d localhost -g 1 -f Json --full --dbuser trafodion --dbpw traf123 -s [schemaname] -t testTopic --sto 20 --interval 10 -c 500

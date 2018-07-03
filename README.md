# Build Process:
KafkaCDC depends some jar files:
1. commons-cli-1.4.jar
2. jdbcT4-2.4.0.jar
3. kafka-clients-1.1.0.jar
4. kafka_2.11-1.1.0.jar
5. log4j-1.2.17.jar
6. scala-reflect-2.11.12.jar
7. slf4j-api-1.7.25.jar
8. slf4j-log4j12-1.7.25.jar
9. zkclient-0.10.jar
10. zookeeper-3.4.10.jar

# KafkaCDC
usage: Consumer Server
* -b,--broker <arg>      bootstrap.servers setting, ex: <node>:9092, default: "localhost:9092"
* -c,--commit <arg>      num message per Kakfa synch, default: 5000
* -e --encode <arg>      character encoding of data, default: utf8
* -f,--format <arg>      format of data, support "Unicom" now, default: ""
* -g,--group <arg>       group for this consumer, default: 0
* -h,--help              show help information
* -p,--partition <arg>   partition parameter format: "id [, id] ...", id should be: "id-id"<br>                        one thread only process data from one partition, default: 16<br/>                        example:<br>			 a. -p "0,4" : means this progress have two threads, process the partition 0 and 4<br>			 b. -p "0-4" : means this progress have five threads, process the partition 0 to 4<br>			 c. -p "0,3-5,6" : means process the partition 0,3,4,5 and 6<br>			 d. -p 4 : means process the partition 0,1,2 and 3
* -s,--schema <arg>      default database schema, use the schema from data without this option, default: null
* -t,--topic <arg>       REQUIRED. topic of subscription
* -v --version           print the version of KafkaCDC
* -z,--zook <arg>        zookeeper connection list, ex: <node>:port[/kafka],...
* -d,--dbip <arg>        database server ip, default: "localhost"
*    --dbport <arg>      database server port, default: 23400
*    --dbuser <arg>      database server user, default: db__root
*    --dbpw <arg>        database server password, default: zz
*    --delim <arg>       field delimiter, default: ','(comma)
*    --bigendian         the data format is big endian, default is little endian
*    --full              pull data from beginning, default: false
*    --interval <arg>    the print state time interval, the unit is second, default: 10s
*    --keepalive         check database keepalive, default is false
*    --key <arg>         key deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer
*    --skip              skip all errors of data, default: false
*    --sto <arg>         kafka poll time-out limit, default: 60s
*    --table <arg>       table name, default: null
*    --tenant <arg>      tanent user name, default: null
*    --value <arg>       value  deserializer, default is: org.apache.kafka.common.serialization.StringDeserializer
*    --zkto <arg>        zookeeper time-out limit, default: 10s

the architecture of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/master/design/architecture.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/architecture.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>
the classes of KafkaCDC as following:<br/>
<p align="center">
<a href="https://github.com/esgyn/kafkaCDC/blob/master/design/classes.jpg" target="_blank">
<img align="center" src="https://github.com/esgyn/kafkaCDC/blob/master/design/classes.jpg" alt="Geolife data at block scale"></a><br/><br/>
</p>

# example:
Must create the schema and tables first of all.
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -full -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -full -t test
* java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -f HongQuan -s SEABASE --table tab -full -t test --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

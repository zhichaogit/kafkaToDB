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
* -c,--commit <arg>      num message per Kakfa synch, default: 500
* -e --encode <arg>      character encoding of data, default: utf8
* -f,--format <arg>      format of data, support "onicom" and "normal" now, default: normal
* -g,--group <arg>       group for this consumer, default: 0
* -p,--partition <arg>   partition parameter format: "id [, id] ...", id should be: "id-id"<br>                        one thread only process data from one partition, default: 16<br/>                        example:<br>			 a. -p "0,4" : means this progress have two threads, process the partition 0 and 4<br>			 b. -p "0-4" : means this progress have five threads, process the partition 0 to 4<br>			 c. -p "0,3-5,6" : means process the partition 0,3,4,5 and 6<br>			 d. -p 4 : means process the partition 0,1,2 and 3
* -s,--schema <arg>      default database schema, use the schema from data without this option, default: null
* -t,--topic <arg>       REQUIRED. topic of subscription
* -z,--zook <arg>        zookeeper connection list, ex: <node>:port[/kafka],...
* -d,--dbip <arg>        database server ip, default: "localhost"
*    --dbport <arg>      database server port, default: 23400
*    --dbuser <arg>      database server user, default: db__root
*    --dbpw <arg>        database server password, default: zz
*    --delim <arg>       field delimiter, default: ','(comma)
*    --interval <arg>    the print state time interval, default: 10s
*    --full              pull data from beginning, default: false
*    --skip              skip all error of data, default: false
*    --sto <arg>         kafka poll time-out limit, default: 60s
*    --table <arg>       kafka poll time-out limit, default: null
*    --tenant <arg>      database tenant user
*    --zkto <arg>        zookeeper time-out limit, default: 10s

Must create the schema and tables first of all.
# example:
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -full -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -full -t test

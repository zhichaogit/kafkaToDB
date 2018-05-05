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
* -f,--format <arg>      format of data, default: unicom
* -g,--group <arg>       group for this consumer, default: 0
* -p,--parallel <arg>    parallel thread number to process message, one thread only process data from one partition, default: 16
* -s,--schema <arg>      default database schema, use the schema from data without this option, default: null
* -t,--topic <arg>       REQUIRED. topic of subscription
* -z,--zook <arg>        zookeeper connection list, ex: <node>:port[/kafka],...
* -d,--dbip <arg>        database server ip, default: "localhost"
*    --dbport <arg>      database server port, default: 23400
*    --dbuser <arg>      database server user, default: db__root
*    --dbpw <arg>        database server password, default: zz
*    --delim <arg>       field delimiter, default: ','(comma)
*    --interval <arg>    the print state time interval, default: 10000ms
*    --full              pull data from beginning, default: false
*    --skip              skip all error of data, default: false
*    --sto <arg>         kafka poll time-out limit, default: 60000ms
*    --table <arg>       kafka poll time-out limit, default: null
*    --zkto <arg>        zookeeper time-out limit, default: 10000ms

Must create the schema and tables first of all.
# example:
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -full -t test

* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -t test
* java -cp bin:bin/\*:libs/\* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table tab -full -t test

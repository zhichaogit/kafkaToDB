KAFKA="$KAFKA"
TOPIC="normal_format_commitLimit512M"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"

DESTSCHEMA="SEABASES1"
TABLE="commitLimit"
TABLEEXP="commitLimitexp"

PARTITION="1"
DELIM=","
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH="$EXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$EXPECTDIR/${TOPIC}_expect.log"
expect <<-EOF
  set timeout 300
  spawn ssh $DBUSER@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$DBPW\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "sqlci <<EOFsql
CREATE SCHEMA IF NOT EXISTS $DESTSCHEMA;
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(C1 INT NOT NULL PRIMARY KEY, C2 VARCHAR(16777216), C3 VARCHAR(16777216), C4 VARCHAR(16777216), C5 VARCHAR(16777216), C6 VARCHAR(16777216), C7 VARCHAR(16777216), C8 VARCHAR(16777216), C9 VARCHAR(16777216), C10 VARCHAR(16777216));
DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(C1 INT NOT NULL PRIMARY KEY, C2 VARCHAR(16777216), C3 VARCHAR(16777216), C4 VARCHAR(16777216), C5 VARCHAR(16777216), C6 VARCHAR(16777216), C7 VARCHAR(16777216), C8 VARCHAR(16777216), C9 VARCHAR(16777216), C10 VARCHAR(16777216));
INSERT INTO $TABLEEXP VALUES(1,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(2,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(3,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(4,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(5,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(6,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(7,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(8,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(9,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
INSERT INTO $TABLEEXP VALUES(10,'col2','col3','col4','col5','col6','col7','col8','col9','col10');
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "1,col2,col3,col4,col5,col6,col7,col8,col9,col10
2,col2,col3,col4,col5,col6,col7,col8,col9,col10
3,col2,col3,col4,col5,col6,col7,col8,col9,col10
4,col2,col3,col4,col5,col6,col7,col8,col9,col10
5,col2,col3,col4,col5,col6,col7,col8,col9,col10
6,col2,col3,col4,col5,col6,col7,col8,col9,col10
7,col2,col3,col4,col5,col6,col7,col8,col9,col10
8,col2,col3,col4,col5,col6,col7,col8,col9,col10
9,col2,col3,col4,col5,col6,col7,col8,col9,col10
10,col2,col3,col4,col5,col6,col7,col8,col9,col10" | sed -e "s/,/${DELIM}/g" > $DATAFILE

existtopic=`$KAFKA/bin/kafka-topics.sh --describe --topic $TOPIC --zookeeper $ZOOKEEPER`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning
KAFKA_CDC="$KAFKA_CDC"
cd $KAFKA_CDC/bin;
echo "./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC --delim "${DELIM}" --mode start --sto 5 --interval 2"
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC --delim "${DELIM}" --mode start --sto 5 --interval 2

#get result file from trafodion
expect <<-EOF
  set timeout 300
  spawn ssh $DBUSER@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$DBPW\r";exp_continue}
  "$ " { send "\r" }
  }
  expect "$ "
  send "mkdir -p  $EXPECTDIR\r"
  expect "$ "
  send "sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
LOG $RESULTPATH;
SELECT c1 FROM $TABLE;
log OFF;
log $EXPECTPATH;
SELECT c1 FROM $TABLEEXP;
log OFF;
DROP TABLE IF EXISTS $TABLE;
DROP TABLE IF EXISTS $TABLEEXP;
EOFsql\r"
  expect "$ "
  send "sed -i \"1d\" $RESULTPATH\r"
  send "sed -i \"1d\" $EXPECTPATH\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF
# clean the environment
CUREXPECTDIR="/tmp"
mkdir -p $CUREXPECTDIR
if [ -f /tmp/${TOPIC}_result.log ];then
 rm -f /tmp/${TOPIC}_result.log
echo "file exist ,delete /tmp/${TOPIC}_result.log"
fi
if [ -f /tmp/${TOPIC}_expect.log ];then
 rm -f /tmp/${TOPIC}_expect.log
echo "file exist , delete /tmp/${TOPIC}_expect.log"
fi

# copy result file to current host
expect <<-EOF
  set timeout 300
  spawn ssh $DBUSER@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$DBPW\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "scp -r $RESULTPATH $EXPECTPATH $CURRENTUSER@$IPADDR:$CUREXPECTDIR\r"
  expect {
  "yes/no" {send "yes\r";exp_continue }
  "password:" { send "$CURRENTUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -f $RESULTPATH $EXPECTPATH\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

RESULTPATH="$CUREXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$CUREXPECTDIR/${TOPIC}_expect.log"
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
# result set：
if [ -f $RESULTPATH -a -f $EXPECTPATH -a "x$(diff -q $RESULTPATH $EXPECTPATH)" == "x" ];then
  echo \"$currentTime $TOPIC expected\" >> $FINALRESULTPATH
  RESULT="$currentTime $TOPIC success"
else
  echo "$currentTime $TOPIC unexpected" >> $FINALRESULTPATH
  RESULT="$currentTime $TOPIC failed"
fi
$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE
rm -f $RESULTPATH
rm -f $EXPECTPATH

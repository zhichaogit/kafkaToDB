KAFKA="$KAFKA"
TOPIC="normal_format_delim_tab"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"
	
DESTSCHEMA="SEABASES1"
TABLE="tab"
TABLEEXP="tabexp"

PARTITION="1"
DELIM= "	"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$SCRIPTSDIR/final.log"
RESULTPATH="$EXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$EXPECTDIR/${TOPIC}_expect.log"
expect <<-EOF
  set timeout 60
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
CREATE TABLE $TABLE(ser NUMERIC NOT NULL PRIMARY KEY, name VARCHAR2(20), age INT, type VARCHAR2(20), yer INT);
DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(ser NUMERIC NOT NULL PRIMARY KEY, name VARCHAR2(20), age INT, type VARCHAR2(20), yer INT);
INSERT INTO $TABLEEXP VALUES(2, 'LC1', 29, 'BC', 2013);
INSERT INTO $TABLEEXP VALUES(3, 'KY1', 22, 'PG', 2013);
INSERT INTO $TABLEEXP VALUES(4, 'MH1', 23, 'PG', 2013);
INSERT INTO $TABLEEXP VALUES(5, 'LX1', 32, 'PG', 2014);
INSERT INTO $TABLEEXP VALUES(6, 'YH2', 36, 'PG', 2013);
INSERT INTO $TABLEEXP VALUES(7, 'LC3', 29, 'BC', 2013);
INSERT INTO $TABLEEXP VALUES(8, 'KY3', 22, 'PG', 2013);
INSERT INTO $TABLEEXP VALUES(9, 'MH3', 23, 'PG', 2013);
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "2,\"LC1\",29,BC,2013
3,\"KY1\",22,PG,2013
4,\"MH1\",23,PG,2013
5,\"LX1\",32,PG,2014
6,\"YH2\",36,PG,2013
7,\"LC3\",29,BC,2013
8,\"KY3\",22,PG,2013
9,\"MH3\",23,PG,2013" | sed -e "s/,/${DELIM}/g" > $DATAFILE

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
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC --delim "${DELIM}" --full start --sto 5 --interval 2

#get result file from $USER
expect <<-EOF
  set timeout 60
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
SELECT * FROM $TABLE;
log OFF;
log $EXPECTPATH;
SELECT * FROM $TABLEEXP;
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
  set timeout 60
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
if [ "x${DEBUG}" != "xYES" ]; then
  rm -f $DATAFILE
  rm -f $RESULTPATH
  rm -f $EXPECTPATH
fi

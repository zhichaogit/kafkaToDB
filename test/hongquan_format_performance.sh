KAFKA="$KAFKA"
TOPIC="g_ad"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"

DESTSCHEMA="SEABASES1"
TABLE="g_ad"

PARTITION="3"
RECORDS="600000"
EXPECTDIR="$EXPECTDIR"
#FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH="$EXPECTDIR/hongquan_format_performance_result.log"
#EXPECTPATH="$EXPECTDIR/hongquan_format_performance_expect.log"

expect <<-EOF
  set timeout 60
  spawn ssh $TRAFODIONUSER@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "sqlci <<EOFsql
CREATE SCHEMA IF NOT EXISTS $DESTSCHEMA;
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
existtopic=`$KAFKA/bin/kafka-topics.sh --describe --topic $TOPIC --zookeeper $ZOOKEEPER`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER

KAFKA_CDC="$KAFKA_CDC"
cd $KAFKA_CDC
javac -d bin -cp example/:libs/* -Xlint:deprecation example/ProducerTest.java
java -cp bin:bin/*:libs/* ProducerTest

cd $KAFKA_CDC/bin
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f HongQuan --bigendian --full start --sto 5 --interval 2 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

#get result file from $TRAFODIONUSER
expect <<-EOF
  set timeout 60
  spawn ssh $TRAFODIONUSER@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue}
  "$ " { send "\r" }
  }
  expect "$ "
  send "mkdir -p  $EXPECTDIR\r"
  expect "$ "
  send "sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
LOG $RESULTPATH;
SELECT count(*) FROM $TABLE;
log OFF;
DROP TABLE IF EXISTS $TABLE;
EOFsql\r"
  expect "$ "
  send "sed -i \"1d\" $RESULTPATH\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF
# clean the environment
CUREXPECTDIR="$SCRIPTSDIR/logs"
if [ -f /tmp/hongquan_format_performance_result.log ];then
 rm -f /tmp/hongquan_format_performance_result.log
echo "file exist ,delete /tmp/${TOPIC}_result.log"
fi

# copy result file to current host
expect <<-EOF
  set timeout 60
  spawn ssh $TRAFODIONUSER@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "scp -r $RESULTPATH $CURRENTUSER@$IPADDR:$CUREXPECTDIR\r"
  expect {
  "yes/no" {send "yes\r";exp_continue }
  "password:" { send "$CURRENTUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -f $RESULTPATH\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF
# result set：
RESULTPATH="$SCRIPTSDIR/logs/hongquan_format_performance_result.log"
$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
RESULT="look the resultfile: $RESULTPATH"
if [ "x${DEBUG}" != "xYES" ]; then
  rm -f $RESULTPATH
  rm -f $EXPECTPATH
fi

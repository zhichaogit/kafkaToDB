KAFKA="$KAFKA"
TOPIC="update_update_row_finally"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"

DESTSCHEMA="SEABASES1"
TABLE="T1"
TABLEEXP="T1EXP"

PARTITION="1"
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
CREATE TABLE $TABLE(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLE VALUES(1, 'update', 'update');

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP VALUES(1, 'update', 'update');

UPDATE $TABLEEXP SET c1 = 1, c2 = 'update1' WHERE c1 = 1 AND c2 = 'update';
UPDATE $TABLEEXP SET c1 = 2, c3 = 'uuu1' WHERE c1 = 1 AND c3 = 'update';
DELETE FROM $TABLEEXP WHERE c1 = 2 AND c2 = 'update1' AND c3 = 'uuu1';
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEU2018-04-02 07:59:54.0856390111update1update
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.0856390212uuu1update
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.085639021update12uuu1" > $DATAFILE

existtopic=`$KAFKA/bin/kafka-topics.sh --describe --topic $TOPIC --zookeeper $ZOOKEEPER`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning

KAFKA_CDC="$KAFKA_CDC"
cd $KAFKA_CDC;

echo "./bin/KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f Unicom --mode start --sto 5 --interval 2"
./bin/KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f Unicom --mode start --sto 5 --interval 2
#get result file from $DBUSER
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
echo "file exist, delete /tmp/${TOPIC}_result.log"
fi
if [ -f /tmp/${TOPIC}_expect.log ];then
 rm -f /tmp/${TOPIC}_expect.log
echo "file exist, delete /tmp/${TOPIC}_expect.log"
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
# result set:
RESULTPATH="$CUREXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$CUREXPECTDIR/${TOPIC}_expect.log"
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
if [ -f $RESULTPATH -a -f $EXPECTPATH -a "x$(diff -q $RESULTPATH $EXPECTPATH)" == "x" ];then
echo "$currentTime $TOPIC expected" >> $FINALRESULTPATH
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

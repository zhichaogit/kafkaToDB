KAFKA="$KAFKA"
TOPIC="unicom_notcommit_when_KMess"
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
INSERT INTO $TABLE VALUES(1, 'I1', '+++');
INSERT INTO $TABLE VALUES(2, 'I2', '+++');
INSERT INTO $TABLE VALUES(3, 'I3', '+++');
INSERT INTO $TABLE VALUES(4, 'I4', '+++');
INSERT INTO $TABLE VALUES(5, 'I5', '+++');

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP VALUES(1, 'I1', '+++');
INSERT INTO $TABLEEXP VALUES(2, 'I2', '+++');
INSERT INTO $TABLEEXP VALUES(3, 'I3', '+++');
INSERT INTO $TABLEEXP VALUES(4, 'I4', '+++');
INSERT INTO $TABLEEXP VALUES(5, 'I5', '+++');


UPDATE $TABLEEXP SET c1 = 5, c2 = 'u5', c3='+++' WHERE c1 = 5;
UPDATE $TABLEEXP SET c1 = 14, c2 = 'k4', c3 ='+++' WHERE c1 = 4;
UPDATE $TABLEEXP SET c1 = 3, c2 = 'u3', c3 ='+++' WHERE c1 = 3;
UPDATE $TABLEEXP SET c1 = 4, c2 = 'k2', c3 ='+++' WHERE c1 = 2;
UPDATE $TABLEEXP SET c1 = 6, c2 = 'k1', c3 ='+++' WHERE c1 = 1;
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEU2018-04-02 07:59:54.0856390551u5I52++++++
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.08563901441k4I42++++++
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEU2018-04-02 07:59:54.0856390331u3I32++++++
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.0856390421k2I22++++++
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.0856390611k1I12++++++" > $DATAFILE

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

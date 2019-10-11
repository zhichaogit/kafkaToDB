KAFKA="$KAFKA"
TOPIC="mulTables_To_oneTable_mapping"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"

DESTSCHEMA="SEABASES1"
TABLE1="T1"
TABLE2="T2"
TABLEEXP1="T1EXP"

PARTITION="1"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH1="$EXPECTDIR/${TABLE1}_result.log"
RESULTPATH2="$EXPECTDIR/${TABLE2}_result.log"
EXPECTPATH1="$EXPECTDIR/${TABLEEXP1}_expect.log"

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
DROP TABLE IF EXISTS $TABLE1;
CREATE TABLE $TABLE1(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));

DROP TABLE IF EXISTS $TABLEEXP1;
CREATE TABLE $TABLEEXP1(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP1 VALUES(1, 'insert1', '+++');
INSERT INTO $TABLEEXP1 VALUES(2, 'insert2', '+++');
INSERT INTO $TABLEEXP1 VALUES(3, 'insert3', '+++');
INSERT INTO $TABLEEXP1 VALUES(4, 'insert4', '+++');


EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337${DESTSCHEMA}.${TABLE1}I2018-04-02 07:59:54.085639011insert12+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337${DESTSCHEMA}.${TABLE1}I2018-04-02 07:59:54.085639021insert22+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337${DESTSCHEMA}.${TABLE2}I2018-04-02 07:59:54.085639031insert32+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337${DESTSCHEMA}.${TABLE2}I2018-04-02 07:59:54.085639041insert42+++" > $DATAFILE

existtopic=`$KAFKA/bin/kafka-topics.sh --describe --topic $TOPIC --zookeeper $ZOOKEEPER`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE

KAFKA_CDC="$KAFKA_CDC"
cd $KAFKA_CDC/bin;
CONFIG="conf/mulTables_To_oneTable_mapping.json"
echo "$TOPDIR/template $PARTITION $BROKER $DBIP $TOPIC $TOPIC "Unicom" $DESTSCHEMA $DESTSCHEMA $DESTSCHEMA $DESTSCHEMA $TABLE2 $TABLE1 $TABLE1 $TABLE1 $KAFKA_CDC/$CONFIG \"\" \"\""
$TOPDIR/template $PARTITION $BROKER $DBIP $TOPIC $TOPIC "Unicom" $DESTSCHEMA $DESTSCHEMA $DESTSCHEMA $DESTSCHEMA $TABLE2 $TABLE1 $TABLE1 $TABLE1 $KAFKA_CDC/$CONFIG "" ""
echo "./KafkaCDC-server.sh --conf $CONFIG"
./KafkaCDC-server.sh --conf $CONFIG

#get result file from $USER
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
LOG $RESULTPATH1;
SELECT * FROM $TABLE1;
log OFF;
log $EXPECTPATH1;
SELECT * FROM $TABLEEXP1;
log OFF;
DROP TABLE IF EXISTS $TABLE1;
DROP TABLE IF EXISTS $TABLEEXP1;
EOFsql\r"
  expect "$ "
  send "sed -i \"1d\" $RESULTPATH1\r"
  send "sed -i \"1d\" $EXPECTPATH1\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

# clean the environment
CUREXPECTDIR="/tmp"
mkdir -p $CUREXPECTDIR
if [ -f /tmp/${TABLE1}_result.log ];then
 rm -f /tmp/${TABLE1}_result.log
echo "file exist, delete /tmp/${TABLE1}_result.log"
fi
if [ -f /tmp/${TABLEEXP1}_expect.log ];then
 rm -f /tmp/${TABLEEXP1}_expect.log
echo "file exist, delete /tmp/${TABLEEXP1}_expect.log"
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
  send "scp -r $RESULTPATH1 $EXPECTPATH1 $CURRENTUSER@$IPADDR:$CUREXPECTDIR\r"
  expect {
  "yes/no" {send "yes\r";exp_continue }
  "password:" { send "$CURRENTUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -f $RESULTPATH1 $EXPECTPATH1\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

#result set:
RESULTPATH="$CUREXPECTDIR/${TABLE1}_result.log"
EXPECTPATH="$CUREXPECTDIR/${TABLEEXP1}_expect.log"
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
  rm -f $RESULTPATH1
  rm -f $EXPECTPATH1
fi

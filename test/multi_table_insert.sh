KAFKA="$KAFKA"
TOPIC="update_insert_row_finally"
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
TABLEEXP2="T2EXP"

PARTITION="1"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH1="$EXPECTDIR/${TABLE1}_result.log"
RESULTPATH2="$EXPECTDIR/${TABLE2}_result.log"
EXPECTPATH1="$EXPECTDIR/${TABLEEXP1}_expect.log"
EXPECTPATH2="$EXPECTDIR/${TABLEEXP2}_expect.log"

expect <<-EOF
  set timeout 60
  spawn ssh $USER@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "sqlci <<EOFsql
CREATE SCHEMA IF NOT EXISTS $DESTSCHEMA;
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE1;
CREATE TABLE $TABLE1(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));

DROP TABLE IF EXISTS $TABLE2;
CREATE TABLE $TABLE2(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));


DROP TABLE IF EXISTS $TABLEEXP1;
CREATE TABLE $TABLEEXP1(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP1 VALUES(4, 'insert4', '+++');
INSERT INTO $TABLEEXP1 VALUES(5, 'insert5', '+++');

DROP TABLE IF EXISTS $TABLEEXP2;
CREATE TABLE $TABLEEXP2(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP2 VALUES(6, 'insert6', '+++');
INSERT INTO $TABLEEXP2 VALUES(7, 'insert7', '+++');

EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLE1I2018-04-02 07:59:54.085639041insert42+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLE1I2018-04-02 07:59:54.085639051insert52+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLE2I2018-04-02 07:59:54.085639061insert62+++
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLE2I2018-04-02 07:59:54.085639071insert72+++" > $DATAFILE

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
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE1,$TABLE2 -t $TOPIC -f Unicom --full start --sto 5 --interval 2

#get result file from $USER
expect <<-EOF
  set timeout 60
  spawn ssh $USER@$DBIP
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
LOG $RESULTPATH1;
SELECT * FROM $TABLE1;
log OFF;
LOG $RESULTPATH2;
SELECT * FROM $TABLE2;
log OFF;
log $EXPECTPATH1;
SELECT * FROM $TABLEEXP1;
log OFF;
log $EXPECTPATH2;
SELECT * FROM $TABLEEXP2;
log OFF;
DROP TABLE IF EXISTS $TABLE1;
DROP TABLE IF EXISTS $TABLE2;
DROP TABLE IF EXISTS $TABLEEXP1;
DROP TABLE IF EXISTS $TABLEEXP2;
EOFsql\r"
  expect "$ "
  send "sed -i \"1d\" $RESULTPATH1\r"
  send "sed -i \"1d\" $RESULTPATH2\r"
  send "sed -i \"1d\" $EXPECTPATH1\r"
  send "sed -i \"1d\" $EXPECTPATH2\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

# clean the environment
CUREXPECTDIR="/tmp"
mkdir -p $CUREXPECTDIR
if [ -f /tmp/${TABLE1}_result.log ];then
 rm -f /tmp/${TABLE1}_result.log
echo "file exist ,delete /tmp/${TABLE1}_result.log"
fi
if [ -f /tmp/${TABLE2}_result.log ];then
 rm -f /tmp/${TABLE2}_result.log
echo "file exist ,delete /tmp/${TABLE2}_result.log"
fi
if [ -f /tmp/${TABLEEXP1}_expect.log ];then
 rm -f /tmp/${TABLEEXP1}_expect.log
echo "file exist , delete /tmp/${TABLEEXP1}_expect.log"
fi
if [ -f /tmp/${TABLEEXP2}_expect.log ];then
 rm -f /tmp/${TABLEEXP2}_expect.log
echo "file exist , delete /tmp/${TABLEEXP2}_expect.log"
fi
# copy result file to current host
expect <<-EOF
  set timeout 60
  spawn ssh $USER@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "scp -r $RESULTPATH1 $RESULTPATH2 $EXPECTPATH1 $EXPECTPATH2 $CURRENTUSER@$IPADDR:$CUREXPECTDIR\r"
  expect {
  "yes/no" {send "yes\r";exp_continue }
  "password:" { send "$CURRENTUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -f $RESULTPATH1 $RESULTPATH2 $EXPECTPATH1 $EXPECTPATH2\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

#result set:
RESULTPATH1="$CUREXPECTDIR/${TABLE1}_result.log"
RESULTPATH2="$CUREXPECTDIR/${TABLE2}_result.log"
EXPECTPATH1="$CUREXPECTDIR/${TABLEEXP1}_expect.log"
EXPECTPATH2="$CUREXPECTDIR/${TABLEEXP2}_expect.log"
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
if [ -f $RESULTPATH1 -a -f $RESULTPATH2 -a -f $EXPECTPATH1 -a -f $EXPECTPATH2 -a "x$(diff -q $RESULTPATH1 $EXPECTPATH1)" == "x" -a "x$(diff -q $RESULTPATH2 $EXPECTPATH2)" == "x" ];then
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
  rm -f $RESULTPATH2
  rm -f $EXPECTPATH1
  rm -f $EXPECTPATH2
fi


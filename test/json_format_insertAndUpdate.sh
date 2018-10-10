KAFKA="$KAFKA"
TOPIC="json_format_insertAndUpdate"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$BROKERIP:$BROKERPORT"
CURRENTUSER="$USER"
CURRENTUSERPS="$CURRENTUSERPS"

DESTSCHEMA="SEABASES1"
TABLE1="t_dpm_outer_account"
TABLE2="t_dpm_outer_account_subset"
TABLE3="t_dpm_outer_account_detail"
TABLEEXP1="t_dpm_outer_accountexp"
TABLEEXP2="t_dpm_outer_account_subsetexp"
TABLEEXP3="t_dpm_outer_account_detailexp"

PARTITION="1"
DELIM=","
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH1="$EXPECTDIR/${TABLE1}_result.log"
RESULTPATH2="$EXPECTDIR/${TABLE2}_result.log"
RESULTPATH3="$EXPECTDIR/${TABLE3}_result.log"
EXPECTPATH1="$EXPECTDIR/${TABLEEXP1}_expect.log"
EXPECTPATH2="$EXPECTDIR/${TABLEEXP2}_expect.log"
EXPECTPATH3="$EXPECTDIR/${TABLEEXP3}_expect.log"
expect <<-EOF
  set timeout 100
  spawn ssh trafodion@$DBIP
   expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "sqlci <<EOFsql
CREATE SCHEMA IF NOT EXISTS $DESTSCHEMA;
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS \"$TABLE1\";
DROP TABLE IF EXISTS \"$TABLE2\";
DROP TABLE IF EXISTS \"$TABLE3\";
DROP TABLE IF EXISTS \"$TABLEEXP1\";
DROP TABLE IF EXISTS \"$TABLEEXP2\";
DROP TABLE IF EXISTS \"$TABLEEXP3\";
CREATE TABLE \"$TABLE1\"(
ACCOUNT_NO VARCHAR(30) NOT NULL,
ACCOUNT_TITLE_NO VARCHAR(20) NOT NULL,
ACCOUNT_NAME VARCHAR(30) CHARACTER SET UTF8,
OPEN_DATE TIMESTAMP,
MEMBER_ID VARCHAR(20),
STATUS_MAP VARCHAR(10),
ACCOUNT_ATTRIBUTE INT,
ACCOUNT_TYPE INT,
CURR_BAL_DIRECTION INT,
BAL_DIRECTION INT,
CURRENCY_CODE VARCHAR(10),
LAST_UPDATE_TIME TIMESTAMP NOT null,
REQUEST_NO VARCHAR(20),
PRIMARY KEY (LAST_UPDATE_TIME)
);
INSERT INTO \"$TABLE1\" VALUES('20010', '2001002001', '100021502592', '2018-04-13 10:06:32','100021502592','1000',1,101,2,2,'CNY','2018-08-02 16:20:23','19587872');

CREATE TABLE \"$TABLE2\"(
UPDATE_TIME TIMESTAMP NOT NULL,
ACCOUNT_NO VARCHAR(30) NOT NULL,
BALANCE VARCHAR(10) NOT NULL,
FUND_TYPE VARCHAR(10),
BALANCE_TYPE VARCHAR(10),
REMARK VARCHAR(10),
CREATE_TIME TIMESTAMP,
PRIMARY KEY (UPDATE_TIME)
);
INSERT INTO \"$TABLE2\" VALUES('2018-08-02 16:19:10','200100',0, 'CR', 'null','null','2018-04-13 10:06:32');

CREATE TABLE \"$TABLE3\"(
TXN_SEQ_NO INT NOT NULL,
SYS_TRACE_NO VARCHAR(30) NOT NULL,
ACCOUNTING_DATE VARCHAR(20),
TXN_TIME TIMESTAMP,
ACCOUNT_NO VARCHAR(30),
TXN_TYPE INT,
TXN_DSCPT VARCHAR(30) CHARACTER SET UTF8,
CHANGE_TYPE VARCHAR(10),
DIRECTION INT,
FROZEN_FLAG VARCHAR(10),
TXN_AMT FLOAT(4),
BEFORE_AMT FLOAT(4),
AFTER_AMT FLOAT(4),
ENTRY_SEQ_NO VARCHAR(10),
OTHER_ACCOUNT_NO VARCHAR(10),
OLD_TXN_SEQ_NO VARCHAR(10),
REMARK VARCHAR(20),
CRDR INT,
PRODUCT_CODE VARCHAR(10),
PAY_CODE VARCHAR(10),
OPERATION_TYPE INT,
DELETE_SIGN VARCHAR(10),
SUITE_NO VARCHAR(10),
CONTEXT_VOUCHER_NO VARCHAR(10),
ACCOUNTING_RULE VARCHAR(10),
TRANSACTION_NO VARCHAR(40),
VOUCHER_NO VARCHAR(30),
PRIMARY KEY (TXN_SEQ_NO)
);

CREATE TABLE \"$TABLEEXP1\"(ACCOUNT_NO VARCHAR(30) NOT NULL,ACCOUNT_TITLE_NO VARCHAR(20) NOT NULL,
ACCOUNT_NAME VARCHAR(30) CHARACTER SET UTF8,
OPEN_DATE TIMESTAMP,
MEMBER_ID VARCHAR(20),
STATUS_MAP VARCHAR(10),
ACCOUNT_ATTRIBUTE INT,
ACCOUNT_TYPE INT,
CURR_BAL_DIRECTION INT,
BAL_DIRECTION INT,
CURRENCY_CODE VARCHAR(10),
LAST_UPDATE_TIME TIMESTAMP NOT null,
REQUEST_NO VARCHAR(20),
PRIMARY KEY (LAST_UPDATE_TIME)
);
INSERT INTO \"$TABLEEXP1\" VALUES('200100200110002150259200001', '2001002001', '100021502592', '2018-04-13 10:06:32','100021502592','1000',1,101,2,2,'CNY','2018-08-02 16:20:39','19587872');

CREATE TABLE \"$TABLEEXP2\"(
UPDATE_TIME TIMESTAMP NOT NULL,
ACCOUNT_NO VARCHAR(30) NOT NULL,
BALANCE VARCHAR(10) NOT NULL,
FUND_TYPE VARCHAR(10),
BALANCE_TYPE VARCHAR(10),
REMARK VARCHAR(10),
CREATE_TIME TIMESTAMP,
PRIMARY KEY (UPDATE_TIME)
);
INSERT INTO \"$TABLEEXP2\" VALUES('2018-08-02 16:20:39','200100200110002150259200001','0.0', 'FR', 'null','null','2018-04-13 10:06:32');

CREATE TABLE \"$TABLEEXP3\"(
TXN_SEQ_NO INT NOT NULL,
SYS_TRACE_NO VARCHAR(30) NOT NULL,
ACCOUNTING_DATE VARCHAR(20),
TXN_TIME TIMESTAMP,
ACCOUNT_NO VARCHAR(30),
TXN_TYPE INT,
TXN_DSCPT VARCHAR(30) CHARACTER SET UTF8,
CHANGE_TYPE VARCHAR(10),
DIRECTION INT,
FROZEN_FLAG VARCHAR(10),
TXN_AMT FLOAT(4),
BEFORE_AMT FLOAT(4),
AFTER_AMT FLOAT(4),
ENTRY_SEQ_NO VARCHAR(10),
OTHER_ACCOUNT_NO VARCHAR(10),
OLD_TXN_SEQ_NO VARCHAR(10),
REMARK VARCHAR(20),
CRDR INT,
PRODUCT_CODE VARCHAR(10),
PAY_CODE VARCHAR(10),
OPERATION_TYPE INT,
DELETE_SIGN VARCHAR(10),
SUITE_NO VARCHAR(10),
CONTEXT_VOUCHER_NO VARCHAR(10),
ACCOUNTING_RULE VARCHAR(10),
TRANSACTION_NO VARCHAR(40),
VOUCHER_NO VARCHAR(30),
PRIMARY KEY (TXN_SEQ_NO)
);
INSERT INTO \"$TABLEEXP3\" VALUES(9396178,'20180802FT017860830','20180802','2018-08-02 16:20:23','200100100120002464209300001',0,'transfer-apply','null',2,'null',50.0000,52.1900,2.1900,'null','null','null','200024642093',1,'50101','FT',1,'null','18750439','null','DR','b18d7839fc1fac4165fd9e0b355c92bd','20180802162026PP030064902');
INSERT INTO \"$TABLEEXP3\" VALUES(9396181,'20180802FT017860830','20180802','2018-08-02 16:20:23','200100200110002150259200001',0,'transfer-apply','null',1,'null',50.0000,9950999,9951049,'null','null','null','100021502592',2,'50101','FT',1,'null','18750439','null','DR','b18d7839fc1fac4165fd9e0b355c92bd','20180802162026PP030064903');
EOFsql\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF
DATAFILE=/tmp/${TOPIC}.data
CLASSPATH=""

echo "{\"database\":\"$DESTSCHEMA\",\"table\":\"$TABLE1\",\"type\":\"update\",\"ts\":1533198039,\"xid\":201480,\"xoffset\":0,\"position\":\"mysql-bin.000075:64570728\",\"data\":{\"ACCOUNT_NO\":\"200100200110002150259200001\",\"ACCOUNT_TITLE_NO\":\"2001002001\",\"ACCOUNT_NAME\":\"100021502592\",\"OPEN_DATE\":\"2018-04-13 10:06:32\",\"MEMBER_ID\":\"100021502592\",\"STATUS_MAP\":\"1000\",\"ACCOUNT_ATTRIBUTE\":1,\"ACCOUNT_TYPE\":101,\"CURR_BAL_DIRECTION\":2,\"BAL_DIRECTION\":2,\"CURRENCY_CODE\":\"CNY\",\"LAST_UPDATE_TIME\":\"2018-08-02 16:20:39\",\"REQUEST_NO\":\"19587872\"},\"old\":{\"LAST_UPDATE_TIME\":\"2018-08-02 16:20:23\"}}" > ${DATAFILE}
echo "{\"database\":\"$DESTSCHEMA\",\"table\":\"$TABLE2\",\"type\":\"update\",\"ts\":1533198023,\"xid\":201195,\"xoffset\":4,\"position\":\"mysql-bin.000075:64487085\",\"data\":{\"ACCOUNT_NO\":\"200100200110002150259200001\",\"BALANCE\":0.0000,\"FUND_TYPE\":\"CR\",\"BALANCE_TYPE\":null,\"REMARK\":null,\"UPDATE_TIME\":\"2018-08-02 16:20:23\",\"CREATE_TIME\":\"2018-04-13 10:06:32\"},\"old\":{\"UPDATE_TIME\":\"2018-08-02 16:19:10\"}}" >> ${DATAFILE}
echo "{\"database\":\"$DESTSCHEMA\",\"table\":\"$TABLE2\",\"type\":\"update\",\"ts\":1533198039,\"xid\":201480,\"xoffset\":1,\"position\":\"mysql-bin.000075:64570948\",\"data\":{\"ACCOUNT_NO\":\"200100200110002150259200001\",\"BALANCE\":0.0000,\"FUND_TYPE\":\"FR\",\"BALANCE_TYPE\":null,\"REMARK\":null,\"UPDATE_TIME\":\"2018-08-02 16:20:39\",\"CREATE_TIME\":\"2018-04-13 10:06:32\"},\"old\":{\"UPDATE_TIME\":\"2018-08-02 16:20:23\"}}" >> ${DATAFILE}
echo "{\"database\":\"$DESTSCHEMA\",\"table\":\"$TABLE3\",\"type\":\"insert\",\"ts\":1533198023,\"xid\":201195,\"xoffset\":5,\"position\":\"mysql-bin.000075:64487480\",\"data\":{\"TXN_SEQ_NO\":9396178,\"SYS_TRACE_NO\":\"20180802FT017860830\",\"ACCOUNTING_DATE\":\"20180802\",\"TXN_TIME\":\"2018-08-02 16:20:23\",\"ACCOUNT_NO\":\"200100100120002464209300001\",\"TXN_TYPE\":0,\"TXN_DSCPT\":\"transfer-apply\",\"CHANGE_TYPE\":null,\"DIRECTION\":2,\"FROZEN_FLAG\":null,\"TXN_AMT\":50.0000,\"BEFORE_AMT\":52.1900,\"AFTER_AMT\":2.1900,\"ENTRY_SEQ_NO\":null,\"OTHER_ACCOUNT_NO\":null,\"OLD_TXN_SEQ_NO\":null,\"REMARK\":\"200024642093\",\"CRDR\":1,\"PRODUCT_CODE\":\"50101\",\"PAY_CODE\":\"FT\",\"OPERATION_TYPE\":1,\"DELETE_SIGN\":null,\"SUITE_NO\":\"18750439\",\"CONTEXT_VOUCHER_NO\":null,\"ACCOUNTING_RULE\":\"DR\",\"TRANSACTION_NO\":\"b18d7839fc1fac4165fd9e0b355c92bd\",\"VOUCHER_NO\":\"20180802162026PP030064902\"}}" >> ${DATAFILE}
echo "{\"database\":\"$DESTSCHEMA\",\"table\":\"$TABLE3\",\"type\":\"insert\",\"ts\":1533198023,\"xid\":201195,\"commit\":true,\"position\":\"mysql-bin.000075:64487875\",\"data\":{\"TXN_SEQ_NO\":9396181,\"SYS_TRACE_NO\":\"20180802FT017860830\",\"ACCOUNTING_DATE\":\"20180802\",\"TXN_TIME\":\"2018-08-02 16:20:23\",\"ACCOUNT_NO\":\"200100200110002150259200001\",\"TXN_TYPE\":0,\"TXN_DSCPT\":\"transfer-apply\",\"CHANGE_TYPE\":null,\"DIRECTION\":1,\"FROZEN_FLAG\":null,\"TXN_AMT\":50.0000,\"BEFORE_AMT\":9950999.0000,\"AFTER_AMT\":9951049.0000,\"ENTRY_SEQ_NO\":null,\"OTHER_ACCOUNT_NO\":null,\"OLD_TXN_SEQ_NO\":null,\"REMARK\":\"100021502592\",\"CRDR\":2,\"PRODUCT_CODE\":\"50101\",\"PAY_CODE\":\"FT\",\"OPERATION_TYPE\":1,\"DELETE_SIGN\":null,\"SUITE_NO\":\"18750439\",\"CONTEXT_VOUCHER_NO\":null,\"ACCOUNTING_RULE\":\"DR\",\"TRANSACTION_NO\":\"b18d7839fc1fac4165fd9e0b355c92bd\",\"VOUCHER_NO\":\"20180802162026PP030064903\"}}" >> ${DATAFILE}

existtopic1=`$KAFKA/bin/kafka-topics.sh --describe --topic $TOPIC --zookeeper $ZOOKEEPER`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi

$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning
KAFKA_CDC="$KAFKA_CDC"
cd $KAFKA_CDC/bin;
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA -t $TOPIC -f Json --full --sto 5 --interval 10 
#get result file from trafodion
expect <<-EOF
  set timeout 60
  spawn ssh trafodion@$DBIP
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
SELECT * FROM \"$TABLE1\";
log OFF;
log $EXPECTPATH1;
SELECT * FROM \"$TABLEEXP1\";
log OFF;
DROP TABLE IF EXISTS \"$TABLE1\";
DROP TABLE IF EXISTS \"$TABLEEXP1\";
LOG $RESULTPATH2;
SELECT * FROM \"$TABLE2\";
log OFF;
log $EXPECTPATH2;
SELECT * FROM \"$TABLEEXP2\";
log OFF;
DROP TABLE IF EXISTS \"$TABLE2\";
DROP TABLE IF EXISTS \"$TABLEEXP2\";
LOG $RESULTPATH3;
SELECT * FROM \"$TABLE3\";
log OFF;
log $EXPECTPATH3;
SELECT * FROM \"$TABLEEXP3\";
log OFF;
DROP TABLE IF EXISTS \"$TABLE3\";
DROP TABLE IF EXISTS \"$TABLEEXP3\";
EOFsql\r"
  expect "$ "
  send "sed -i \"1d\" $RESULTPATH1\r"
  send "sed -i \"1d\" $RESULTPATH2\r"
  send "sed -i \"1d\" $RESULTPATH3\r"
  send "sed -i \"1d\" $EXPECTPATH1\r"
  send "sed -i \"1d\" $EXPECTPATH2\r"
  send "sed -i \"1d\" $EXPECTPATH3\r"
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
if [ -f /tmp/${TABLE3}_result.log ];then
 rm -f /tmp/${TABLE3}_result.log
echo "file exist ,delete /tmp/${TABLE3}_result.log"
fi
if [ -f /tmp/${TABLEEXP1}_expect.log ];then
 rm -f /tmp/${TABLEEXP1}_expect.log
echo "file exist , delete /tmp/${TABLEEXP1}_expect.log"
fi
if [ -f /tmp/${TABLEEXP2}_expect.log ];then
 rm -f /tmp/${TABLEEXP2}_expect.log
echo "file exist , delete /tmp/${TABLEEXP2}_expect.log"
fi
if [ -f /tmp/${TABLEEXP3}_expect.log ];then
 rm -f /tmp/${TABLEEXP3}_expect.log
echo "file exist , delete /tmp/${TABLEEXP3}_expect.log"
fi

# copy result file to current host
expect <<-EOF
  set timeout 60
  spawn ssh trafodion@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "scp -r $RESULTPATH1 $EXPECTPATH1 $RESULTPATH2 $EXPECTPATH2 $RESULTPATH3 $EXPECTPATH3  $CURRENTUSER@$IPADDR:$CUREXPECTDIR\r"
  expect {
  "yes/no" {send "yes\r";exp_continue }
  "password:" { send "$CURRENTUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -f $RESULTPATH1 $EXPECTPATH1 $RESULTPATH2 $EXPECTPATH2 $RESULTPATH3 $EXPECTPATH3\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF
# result set：
RESULTPATH1="$CUREXPECTDIR/${TABLE1}_result.log"
RESULTPATH2="$CUREXPECTDIR/${TABLE2}_result.log"
RESULTPATH3="$CUREXPECTDIR/${TABLE3}_result.log"
EXPECTPATH1="$CUREXPECTDIR/${TABLEEXP1}_expect.log"
EXPECTPATH2="$CUREXPECTDIR/${TABLEEXP2}_expect.log"
EXPECTPATH3="$CUREXPECTDIR/${TABLEEXP3}_expect.log"
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
if [ -f $RESULTPATH1 -a -f $RESULTPATH2 -a -f $RESULTPATH3 -a -f $EXPECTPATH1 -a -f $EXPECTPATH2 -a -f $EXPECTPATH3 -a "x$(diff -q $RESULTPATH1 $EXPECTPATH1)" == "x" -a "x$(diff -q $RESULTPATH2 $EXPECTPATH2)" == "x" -a "x$(diff -q $RESULTPATH3 $EXPECTPATH3)" == "x" ];then
  echo \"$currentTime $TOPIC expected\" >> $FINALRESULTPATH
  RESULT="$currentTime $TOPIC success"
else
  echo "$currentTime $TOPIC unexpected" >> $FINALRESULTPATH
  RESULT="$currentTime $TOPIC failed"
fi
$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE
rm -r $RESULTPATH1
rm -r $RESULTPATH2
rm -r $RESULTPATH3
rm -r $EXPECTPATH1
rm -r $EXPECTPATH2
rm -r $EXPECTPATH3

KAFKA="$KAFKA"
TOPIC="normal_format"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$IPADDR:9092"

SCHEMA="S1"
DESTSCHEMA="SEABASE"
TABLE="emps"
TABLEEXP="empsexp"

PARTITION="1"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH="$EXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$EXPECTDIR/${TOPIC}_expect.log"


su - trafodion<<EOFsu
sqlci <<EOFsql
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
EOFsql
exit;
EOFsu
DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "2,\"LC1\",29,BC,2013
3,\"KY1\",22,PG,2013
4,\"MH1\",23,PG,2013
5,\"LX1\",32,PG,2014
6,\"YH2\",36,PG,2013
7,\"LC3\",29,BC,2013
8,\"KY3\",22,PG,2013
9,\"MH3\",23,PG,2013" > $DATAFILE

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
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC --full --sto 5 --interval 2
# clean the environment
if [ -f $RESULTPATH ];then
rm -f $RESULTPATH
echo "file exist ,delete $RESULTPATH"
fi
if [ -f $EXPECTPATH ];then
rm -f $EXPECTPATH
echo "file exist , delete $EXPECTPATH"
fi

su - trafodion <<EOFsu
if [ ! -d $EXPECTDIR ];then
mkdir $EXPECTDIR
fi
sqlci <<EOFsql
SET SCHEMA SEABASE;
LOG $RESULTPATH;
SELECT * FROM $TABLE;
log OFF;
log $EXPECTPATH;
SELECT * FROM $TABLEEXP;
log OFF;
DROP TABLE IF EXISTS $TABLE;
DROP TABLE IF EXISTS $TABLEEXP;
EOFsql
exit;
EOFsu
sed -i "1d" $RESULTPATH
sed -i "1d" $EXPECTPATH
# result set：
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
if [ -f $RESULTPATH -a -f $EXPECTPATH -a "x$(diff -q $RESULTPATH $EXPECTPATH)" == "x" ];then
echo "$currentTime $TOPIC expected" >> $FINALRESULTPATH
RESULT="$currentTime $TOPIC success"
else
echo "$currentTime $TOPIC unexpected" >> $FINALRESULTPATH
RESULT="$currentTime $TOPIC failed"
fi
$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE
rm -f $RESULTPATH
rm -f $EXPECTPATH

KAFKA="$KAFKA"
TOPIC="g_ad"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$IPADDR:9092"

DESTSCHEMA="SEABASE"
TABLE="g_ad"

PARTITION="3"
RECORDS="600000"
EXPECTDIR="$EXPECTDIR"
#FINALRESULTPATH="$EXPECTDIR/final.log"
RESULTPATH="$EXPECTDIR/hongquan_format_performance_result.log"
#EXPECTPATH="$EXPECTDIR/hongquan_format_performance_expect.log"

su - trafodion<<EOFsu
sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));
EOFsql
exit;
EOFsu

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
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f HongQuan --bigendian --full --sto 5 --interval 2 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# clean the environment
if [ -f $EXPECTPATH ];then
rm -rf $EXPECTPATH
echo "file exist ,delete $EXPECTPATH"
fi

if [ -f $RESULTPATH ];then
rm -f $RESULTPATH
echo "file exist ,delte $RESULTPATH"
fi

if [ ! -d $EXPECTDIR ];then
mkdir $EXPECTDIR
fi

su - trafodion<<EOFsu
sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
LOG $RESULTPATH;
SELECT count(*) FROM $TABLE;
log OFF;
DROP TABLE IF EXISTS $TABLE;
EOFsql
exit;
EOFsu

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
RESULT="look the resultfile: $RESULTPATH"
#rm -f $RESULTPATH

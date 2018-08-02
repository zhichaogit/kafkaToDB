KAFKA="$KAFKA"
TOPIC="g_ad"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$IPADDR:9092"

DESTSCHEMA="SEABASE"
TABLE="g_ad"
TABLEEXP="g_adexp"

PARTITION="1"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH="$EXPECTDIR/hongquan_format_varchar_type_result.log"
EXPECTPATH="$EXPECTDIR/hongquan_format_varchar_type_expect.log"

su - trafodion<<EOFsu
sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));
UPSERT INTO $TABLEEXP VALUES(50462976, 155, 134678021, 9, 10, 235736075, 'aa');
UPSERT INTO $TABLEEXP VALUES(50462976, 155, 134678021, 9, 10, 235736075, 'aaaaa');
UPSERT INTO $TABLEEXP VALUES(50462976, 155, 134678021, 9, 10, 235736075, 'aaaaa');
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

./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f HongQuan --full --sto 5 --interval 2 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# clean the environment
if [ -f $EXPECTPATH ];then
rm -rf $EXPECTPATH
echo "file exist ,delete $EXPECTPATH"
fi

if [ -f $RESULTPATH ];then
rm -f $RESULTPATH
echo "file exist ,delte $RESULTPATH"
fi

su - trafodion <<EOFsu
if [ ! -d $EXPECTDIR ];then
mkdir $EXPECTDIR
fi

sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
LOG $RESULTPATH;
SELECT * FROM $TABLE order by VALUE;
log OFF;
log $EXPECTPATH;
SELECT * FROM $TABLEEXP order by VALUE;
log OFF;
DROP TABLE IF EXISTS $TABLE;
DROP TABLE IF EXISTS $TABLEEXP;
EOFsql
exit;
EOFsu

sed -i "1d" $RESULTPATH
sed -i "1d" $EXPECTPATH
#result set:

currentTime=$(date "+%Y-%m-%d %H:%M:%S")
if [ -f $RESULTPATH -a -f $EXPECTPATH -a "x$(diff -q $RESULTPATH $EXPECTPATH)" == "x" ];then
echo "$currentTime hongquan_format_varchar_type expected" >> $FINALRESULTPATH
RESULT="$currentTime hongquan_format_varchar_type success"
else
echo "$currentTime hongquan_format_varchar_type unexpected" >> $FINALRESULTPATH
RESULT="$currentTime hongquan_format_varchar_type failed"
fi

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $RESULTPATH
rm -f $EXPECTPATH

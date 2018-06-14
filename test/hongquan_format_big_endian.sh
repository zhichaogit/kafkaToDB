KAFKA="/work/kafka/kafka_2.11-1.1.0"
TOPIC="g_ad"
IPADDR="192.168.0.71"
ZOOKEEPER="$IPADDR:2181"
BROKER="$IPADDR:9092"

DESTSCHEMA="SEABASE"
TABLE="g_ad"
TABLEEXP="g_adexp"

PARTITION="1"

sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(DataID INT, Type TINYINT UNSIGNED, DataTime INT, SysID TINYINT UNSIGNED, version TINYINT UNSIGNED, SaveTime INT, Value VARCHAR(5));
UPSERT INTO $TABLEEXP VALUES(66051, 155, 84281096, 9, 10, 185339150, 'aa');
UPSERT INTO $TABLEEXP VALUES(66051, 155, 84281096, 9, 10, 185339150, 'aaaaa');
UPSERT INTO $TABLEEXP VALUES(66051, 155, 84281096, 9, 10, 185339150, 'aaaaa');
EOF

DATAFILE=/tmp/$TOPIC.data
existtopic=`$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER|grep $TOPIC`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
java -cp bin:bin/*:libs/* ProducerTest

KAFKA_CDC="/work/kafka/KafkaCDC"
java -cp $KAFKA_CDC/bin/:$KAFKA_CDC/libs/* KafkaCDC -p $PARTITION -b $BROKER -d $IPADDR -s $DESTSCHEMA --table $TABLE -t $TOPIC -f HongQuan --bigendian --full --sto 5 --interval 2 --key org.apache.kafka.common.serialization.LongDeserializer --value org.apache.kafka.common.serialization.ByteArrayDeserializer

# clean the environment
sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
SELECT * FROM $TABLE;
SELECT * FROM $TABLEEXP;
EOF

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC


KAFKA="/work/kafka/kafka_2.11-1.1.0"
TOPIC="normal_format"
IPADDR="192.168.0.71"
ZOOKEEPER="$IPADDR:2181"
BROKER="$IPADDR:9092"

SCHEMA="S1"
DESTSCHEMA="SEABASE"
TABLE="emps"

PARTITION="1"

sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(ser NUMERIC NOT NULL PRIMARY KEY, name VARCHAR2(20), age INT, type VARCHAR2(20), yer INT);
EOF

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

existtopic=`$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER|grep $TOPIC`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning

KAFKA_CDC="/work/kafka/KafkaCDC"
java -cp $KAFKA_CDC/bin/:$KAFKA_CDC/libs/* KafkaCDC -p $PARTITION -b $BROKER -d $IPADDR -s $DESTSCHEMA --table $TABLE -t $TOPIC -f normal --full --sto 5 --interval 2

# clean the environment
sqlci <<EOF
SET SCHEMA SEABASE;
SELECT * FROM $TABLE;
DROP TABLE IF EXISTS $TABLE;
EOF

# result set：
echo "expect results:

--- 0 row(s) selected."

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE


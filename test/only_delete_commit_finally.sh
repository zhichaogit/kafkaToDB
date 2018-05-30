KAFKA="/work/kafka/kafka_2.11-1.1.0"
TOPIC="only_insert_commit_all"
IPADDR="192.168.0.71"
ZOOKEEPER="$IPADDR:2181"
BROKER="$IPADDR:9092"

SCHEMA="S1"
DESTSCHEMA="SEABASE"
TABLE="T1"
TABLEEXP="T1EXP"

PARTITION="1"

sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLE VALUES(5, 'delete', 'delete');

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP VALUES(5, 'delete', 'delete');
DELETE FROM $TABLEEXP WHERE c1 = 5;
DELETE FROM $TABLEEXP WHERE c1 is null;
DELETE FROM $TABLEEXP WHERE c1 = 5;
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.085639051bbb2)))
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.08563901delete2delete
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.085639051delete2delete" > $DATAFILE

existtopic=`$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER|grep $TOPIC`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning

KAFKA_CDC="/work/kafka/KafkaCDC"
java -cp $KAFKA_CDC/bin/:$KAFKA_CDC/libs/* KafkaCDC -p $PARTITION -b $BROKER -d $IPADDR -s $DESTSCHEMA --table $TABLE -t $TOPIC -f Unicom --full --sto 5 --interval 2

# clean the environment
sqlci <<EOF
SET SCHEMA SEABASE;
SELECT * FROM $TABLE;
SELECT * FROM $TABLEEXP;
DROP TABLE IF EXISTS $TABLE;
DROP TABLE IF EXISTS $TABLEEXP;
EOF

# result set：
echo "expect results:

--- 0 row(s) selected."

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE


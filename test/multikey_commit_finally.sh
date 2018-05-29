KAFKA="/work/kafka/kafka_2.11-1.1.0"
TOPIC="only_insert_commit_all"
IPADDR="192.168.0.71"
ZOOKEEPER="$IPADDR:2181"
BROKER="$IPADDR:9092"

SCHEMA="S1"
DESTSCHEMA="SEABASE"
TABLE="T1"

PARTITION="1"

sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(c1 INT NOT NULL, c2 VARCHAR(10) NOT NULL, c3 VARCHAR(10), c4 INT, PRIMARY KEY (c2, c1));
INSERT INTO $TABLE VALUES(1, 'aaa', 'delete', 111);
INSERT INTO $TABLE VALUES(2, 'bbb', 'update', 222);
INSERT INTO $TABLE VALUES(3, 'ccc', 'insert', 333);
INSERT INTO $TABLE VALUES(4, 'ddd', 'updkey', 444);
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.085639011aaa2delete3111
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEU2018-04-02 07:59:54.0856390221bbbbbb2update1update32222222
CRM_CUE2159140509447540000000058000191620222522,973765081000000005800019142335$TABLEI2018-04-02 07:59:54.085639031ccc2(((
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.0856390541eeeddd2updkey1update" > $DATAFILE

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
DROP TABLE IF EXISTS $TABLE;
EOF

# result set：
echo "expect results:

C1           C2          C3          C4
-----------  ----------  ----------  -----------

          2  bbb         update1            2222
          3  ccc         (((                   ?
          5  eee         updkey1             444

--- 3 row(s) selected."

$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
rm -f $DATAFILE


KAFKA="$KAFKA"
TOPIC="update_insert_row_finally"
IPADDR="$IPADDR"
ZKIP="$ZKIP"
DBIP="$DBIP"
ZOOKEEPER="$ZKIP:2181"
BROKER="$IPADDR:9092"

SCHEMA="S1"
DESTSCHEMA="SEABASE"
TABLE="T1"
TABLEEXP="T1EXP"

PARTITION="1"
EXPECTDIR="$EXPECTDIR"
FINALRESULTPATH="$FINALRESULTPATH"
RESULTPATH="$EXPECTDIR/${TOPIC}_result.log"
EXPECTPATH="$EXPECTDIR/${TOPIC}_expect.log"

su - trafodion<<EOFsu
sqlci <<EOFsql
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLE VALUES(1, 'insert', 'insert');

DROP TABLE IF EXISTS $TABLEEXP;
CREATE TABLE $TABLEEXP(c1 INT NOT NULL, c2 VARCHAR(10), c3 VARCHAR(10), PRIMARY KEY (c1));
INSERT INTO $TABLEEXP VALUES(1, 'insert', 'insert');

UPSERT INTO $TABLEEXP VALUES(1, 'ddd', '+++');
UPDATE $TABLEEXP SET c1 = 2, c2 = 'uuu1' WHERE c1 = 1;
DELETE FROM $TABLEEXP WHERE c1 = 2;
EOFsql
exit;
EOFsu

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLEI2018-04-02 07:59:54.085639011ddd2+++
CRM_CUE2159140509447540000000058000191684022522,973813001000000005800019142338$TABLEK2018-04-02 07:59:54.0856390211uuu1eee
CRM_CUE2159140509447540000000058000191662722522,973800561000000005800019142337$TABLED2018-04-02 07:59:54.085639021delete2delete" > $DATAFILE

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
./KafkaCDC-server.sh -p $PARTITION -b $BROKER -d $DBIP -s $DESTSCHEMA --table $TABLE -t $TOPIC -f Unicom --full --sto 5 --interval 2

# clean the environment
if [ -f $RESULTPATH ];then
rm -f $RESULTPATH
echo "file exist ,delete $RESULTPATH"
fi
if [ -f $EXPECTPATH ];then
rm -f $EXPECTPATH
echo "file exist ,delte $EXPECTPATH"
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
#result set:
currentTime=$(date "+%Y-%m-%d %H:%M:%S")
diff -q $RESULTPATH $EXPECTPATH
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

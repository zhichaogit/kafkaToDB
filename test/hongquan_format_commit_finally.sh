KAFKA="/work/kafka/kafka_2.11-1.1.0"
TOPIC="hongquan_format"
IPADDR="192.168.0.71"
ZOOKEEPER="$IPADDR:2181"
BROKER="$IPADDR:9092"

DESTSCHEMA="SEABASE"
TABLE="g_ad"

PARTITION="1"

sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE(DataID INT, Type CHAR, DataTime INT, SysID CHAR, version CHAR, SaveTime INT, Value INT);
EOF

DATAFILE=/tmp/$TOPIC.data
CLASSPATH=""
echo "S�L�Z��Z��L   {
S�L�Z��%Z��L   {
S�L�Z��/Z��[   {
S�L�Z��9Z��j   {
S�L�Z��DZ��j   {
S�L�Z��NZ��y   {
S�L�Z��XZ���   {
S�L�Z��bZ���   {
S�L�Z��lZ���   {
S�L�Z��vZ���   {
S�L�Z���Z���   {
S�L�Z���Z���   {
S��[נZ��$   �
S��[תZ��4   �
S��[׵Z��4   �
S��[׿Z��C   �
S��[��Z��R   �
S��[��Z��R   �
S��[��Z��f   �
S��[��Z��p   �" > $DATAFILE
existtopic=`$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER|grep $TOPIC`
if [ "x$existtopic" != "x" ]; then
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
fi
$KAFKA/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions $PARTITION --topic $TOPIC
$KAFKA/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER
$KAFKA/bin/kafka-console-producer.sh --broker-list $BROKER --key-serializer org.apache.kafka.common.serialization.LongDeserializer --value-serializer org.apache.kafka.common.serialization.ByteArrayDeserializer --topic $TOPIC < $DATAFILE
#$KAFKA/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning

KAFKA_CDC="/work/kafka/KafkaCDC"
java -cp $KAFKA_CDC/bin/:$KAFKA_CDC/libs/* KafkaCDC -p $PARTITION -b $BROKER -d $IPADDR -s $DESTSCHEMA --table $TABLE -t $TOPIC --delim "4|1|4|1|1|4|4" -f HongQuan --full --sto 5 --interval 2

# clean the environment
sqlci <<EOF
SET SCHEMA $DESTSCHEMA;
SELECT * FROM $TABLE;
EOF

#$KAFKA/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $TOPIC
#rm -f $DATAFILE


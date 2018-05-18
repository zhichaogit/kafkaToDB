rm -rf bin/*.class bin/*.jar *.jar
cp src/log4j.* bin/
javac -d bin -cp src:libs/* -Xlint:deprecation src/*.java

# package
rm -rf KafkaCDC.tar.gz
rm -rf KafkaCDC
mkdir -p KafkaCDC/bin
mkdir -p KafkaCDC/log
cp -r libs KafkaCDC/
cp bin/*.class KafkaCDC/bin
cp bin/log4j.* KafkaCDC/bin
cp README KafkaCDC/

tar zcvf KafkaCDC.tar.gz KafkaCDC
rm -rf KafkaCDC

#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table emps -t test -f normal --full
#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table TF_F_ACCOUNT -t test1 -f unicom --full
#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table TF_F_USER_ITEM -t test2 -f unicom --full --sto 5 --interval 2
#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table TS_B_BILL -t test4 -f unicom --full --sto 3 --interval 2
#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -t test5 -f unicom --full --sto 3 --interval 2
java -cp bin:bin/*:libs/* KafkaCDC -p 2 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE -t test5 -f unicom --full --sto 3 --interval 2
#java -cp bin:bin/*:libs/* KafkaCDC -p 2 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table TF_F_USER_ITEM -t test -f unicom --full
#java -cp bin:bin/*:libs/* KafkaCDC -p 1 -b 192.168.0.71:9092 -d 192.168.0.71 -g 1 -s SEABASE --table TD_B_PARTY_PRODUCT -t test3 -f unicom --full

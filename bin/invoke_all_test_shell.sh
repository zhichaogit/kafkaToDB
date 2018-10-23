#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run kafkacdc/test  scripts 
#

# you'll have to change the the following conf at least if you want this
#test to run:
# 1.example/ProducerTest.java (host:port)
# 2.$(KAFKA,KAFKA_CDC,ZKIP,DBIP,BROKERIP,BROKERPORT,TRAFODIONUSERPS,
#CURRENTUSERPS,)
#if there are error or faild ,you can go $SCRIPTSDIR/logs for more INFO
# 3. the IP better not be write "localhost"

#kafka path
KAFKA="/usr/hdp/2.4.2.0-258/kafka"
#kafkaCDC path
KAFKA_CDC="/home/$USER/kafkaCDC/target/KafkaCDC"

#current host IP
IPADDR=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/')
#zookeeper IP
ZKIP="localhost"
#trafodion/esgyndb ip
DBIP="localhost"
#kafka ip
BROKERIP="localhost"
BROKERPORT="6667"
#user(trafci) password
TRAFODIONUSERPS="traf123"
#user(kafkaCDCuser) password
CURRENTUSERPS="esgyndb"
#expectResult and realResult path
EXPECTDIR="/tmp/kafkaCDClogs"
#kafkaCDC scripts path
SCRIPTSDIR="$KAFKA_CDC/test"
#the finally result (success or failed ) path
FINALRESULTPATH="$SCRIPTSDIR/logs/final.log"

# make sure installed tcl
if [ "x$(rpm -qa | grep tcl)" == "x" ];then
  sudo yum install -y tcl
fi
#make sure installed expect
if [ "x$(rpm -qa | grep expect)" == "x" ];then
  sudo yum install -y expect
fi
#ready env
expect <<-EOF
  log_user 0
  set timeout 10
  spawn ssh trafodion@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "mkdir -p $EXPECTDIR\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

if [ -d ${FINALRESULTPATH} ];then
  rm -rf ${FINALRESULTPATH}
fi
if [ ! -d ${SCRIPTSDIR}/logs ];then
  mkdir -p ${SCRIPTSDIR}/logs
fi
if [ "$EXPECTDIR" == "/tmp" -o "$EXPECTDIR" == "/tmp/" ];then
  echo "Not recommended set \"EXPECTDIR\" to \"/tmp\",you can set it like [/tmp/kafkaCDC/logs]" 
  exit 0
fi
if [ "$IPADDR" == "localhost" ];then
  echo "\"IPADDR\" should IP,not \"localhost\""
  exit 0
fi
#foreach exec the shell script
for script in ${SCRIPTSDIR}/*sh
do
filePath=${script##*/}
echo "$filePath is running ......."
LOGPATH="${script%/*}/logs/${filePath%.*}_INFO.log"

. $script >${LOGPATH} 2>${LOGPATH}
echo " $RESULT"
done
#clean env
expect <<-EOF
  log_user 0
  set timeout 10
  spawn ssh trafodion@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$TRAFODIONUSERPS\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -rf $EXPECTDIR\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

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
# 2.$(KAFKA,KAFKA_CDC,ZKIP,DBIP,BROKERIP,BROKERPORT,DBPW,
#CURRENTUSERPS,)
#if there are error or faild ,you can go $SCRIPTSDIR/logs for more INFO
# 3. the IP better not be write "localhost"

#kafka path
KAFKA="$MY_SW_HOME/kafka_2.11-1.1.0"
#kafkaCDC path
TOPDIR=`pwd`
KAFKA_CDC="$TOPDIR/../target/KafkaCDC"

#current host IP
IPADDR=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/')
#zookeeper IP
ZKIP="localhost"
#$USER/database ip
DBIP="localhost"
#kafka ip
BROKERIP="localhost"
BROKERPORT="9092"
#trafodiion user
DBUSER="wangxz"
#user(trafci) password
DBPW="traf123"
#user(kafkaCDCuser) password
CURRENTUSERPS="wangxz"
#expectResult and realResult path
EXPECTDIR="/tmp/kafkaCDClogs"
#kafkaCDC scripts path
SCRIPTSDIR="$TOPDIR"
#the finally result (success or failed ) path
FINALRESULTPATH="$SCRIPTSDIR/logs/final.log"
DEBUG="YES"

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
  set timeout 300
  spawn ssh $DBUSER@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$DBPW\r";exp_continue }
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
for script in `ls ${TOPDIR}/*.sh | grep -v ${TOPDIR}/invoke_all_test_shell.sh`
do
filePath=${script##*/}
echo "$filePath is running ......."
LOGPATH="${TOPDIR}/logs/${filePath%.*}_INFO.log"

. $script $DEBUG >${LOGPATH} 2>${LOGPATH}
echo " $RESULT"
done
#clean env
expect <<-EOF
  log_user 0
  set timeout 300
  spawn ssh $DBUSER@$DBIP
  expect {
  "yes/no" { send "yes\r";exp_continue }
  "password:" { send "$DBPW\r";exp_continue }
  "$ " { send "\r" }
  }
  expect "$ "
  send "rm -rf $EXPECTDIR\r"
  expect "$ "
  send "exit\r"
  expect eof
EOF

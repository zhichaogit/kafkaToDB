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

KAFKA="/opt/kafka_2.11-0.10.0.0"
KAFKA_CDC="/opt/KafkaCDC"

IPADDR="localhost"
ZKIP="localhost"
DBIP="localhost"
EXPECTDIR="/opt/trafodion/log"
FINALRESULTPATH="$EXPECTDIR/final.log"

SCRIPTSDIR="/opt/KafkaCDC/test"

#ready env
su - trafodion <<EOF
if [ ! -d $EXPECTDIR ];then
mkdir $EXPECTDIR
fi
exit;
EOF

if [ ! -d ${SCRIPTSDIR}/logs ];then
mkdir ${SCRIPTSDIR}/logs
fi

if [ -f $FINALKRESULTTPATH ];then
rm -f $FINALRESULTPATH
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


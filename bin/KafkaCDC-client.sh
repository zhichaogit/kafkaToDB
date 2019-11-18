#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Operational environment preparation 

#1.make sure env
if [ "x$(uname)" == "xLinux" ];
then
JAVA_HOME=${JAVA_HOME-"/usr/bin/java"}
fi
bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`

BASEPATH=$(cd $bin ; cd ../; pwd)
LIBSPATH=$(cd $BASEPATH/libs ; pwd)

echo "bin:${bin}"
echo "java_home:$JAVA_HOME"
echo "BASEPATH:${BASEPATH}"
echo "LIBSPATH:${LIBSPATH}"

#2 drop logs just reserve 3
if [ -d $BASEPATH/logs ];then
  LOGSPATH="$BASEPATH/logs"
  echo "LOGSPATH:${LOGSPATH}"
  . $BASEPATH/bin/droplogs.sh
fi
#3 analy parameters
execCommand="java -Djava.ext.dirs=${LIBSPATH}:$JAVA_HOME/jre/lib/ext -jar $LIBSPATH/KafkaCDC-client.jar"

if [ $# = 0 ]; then
  execCommand="$execCommand -h"
  exec ${execCommand}
  exit
fi

#4. exec jar file
exec ${execCommand} "${@}"

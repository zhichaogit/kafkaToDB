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

cd ${BASEPATH}
#2 analy parameters
execCommand="java -jar $LIBSPATH/KafkaCDC.jar"

if [ $# = 0 ]; then
  execCommand="$execCommand -h"
  exec ${execCommand}
  exit
fi
whitespace="[[:space:]]"
blank_str=" "
   for i in "$@"
    do  
        case x"$i" in
        x" ")
        i="\"space\""
        execCommand=${execCommand}${blank_str}${i}
        ;;  
        x"	")
        i="\"tab\""
	execCommand=${execCommand}${blank_str}${i}
        ;;  
        *)  
        execCommand=${execCommand}${blank_str}${i}
        esac
  done
#3. exec jar file
exec ${execCommand} 
>>>>>>> 01878d4... fix the bug:FileNotFOundException on log4jxml is returned when started KafkaCDC.

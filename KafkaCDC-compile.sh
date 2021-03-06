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
echo "java_home:$JAVA_HOME"

compilebasepath=$(cd `dirname $0`; pwd)
echo "compilePath:$compilebasepath"

#2.mvn clean package
cd $compilebasepath ;mvn clean package

#3. unzip tar.gz
tar -zxvf ./target/KafkaCDC.tar.gz -C ./target
echo "tar.gz has unzip, you should go to ./target"

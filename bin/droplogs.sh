#!/bin/bash

#reserve num
ReservedNum=3
dropFileDir=$LOGSPATH
date=$(date "+%Y%m%d-%H%M%S")

FileNum=$(ls -l $dropFileDir|grep ^d |wc -l)

while(( $FileNum > $ReservedNum))
do
    OldFile=$(ls -lrt $dropFileDir|grep ^d|awk '{print $(NF-1),$NF}'| head -1)
    #echo  $date "Delete File:"$OldFile
    rm -rf $dropFileDir/"$OldFile" 
    let "FileNum--"
done 

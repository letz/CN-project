#!/bin/bash

class_name=`echo "$2" | cut -d '.' -f 1`
map=`echo "$class_name" | tr '[:upper:]' '[:lower:]'`

if [ $1 == "create" ];then
	hdfs dfs -mkdir /user/$USER/$map/
	hdfs dfs -mkdir /user/$USER/$map/input
	hdfs dfs -put $3 /user/$USER/$map/input
elif [ $1 == "clean" ];then
	hdfs dfs -rm /user/$USER/$map/output/*
	hdfs dfs -rmdir /user/$USER/$map/output
	rm *.jar *.class
elif [ $1 == "clean-all" ];then
	hdfs dfs -rm /user/$USER/$map/output/*
	hdfs dfs -rmdir /user/$USER/$map/output
	hdfs dfs -rm /user/$USER/$map/input/*
	hdfs dfs -rmdir /user/$USER/$map/input
	hdfs dfs -rmdir /user/$USER/$map/
	rm *.jar *.class
elif [ $1 == "run" ];then
	hadoop com.sun.tools.javac.Main Log.java $2
	jar cf ws.jar *.class
	hadoop jar ws.jar $class_name /user/$USER/$map/input /user/$USER/$map/output
elif [ $1 == "results" ];then
	hdfs dfs -cat /user/$USER/$map/output/part-r-00000
else
	echo "Available commands: create | clean | clean-all | run | results"
fi

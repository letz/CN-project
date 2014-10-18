#!/bin/bash

if [ $1 == "create" ];then
	hdfs dfs -mkdir /user/$USER/$2/
	hdfs dfs -mkdir /user/$USER/$2/input
	hdfs dfs -put $3 /user/$USER/$2/input
elif [ $1 == "clean" ];then
	hdfs dfs -rm /user/$USER/$2/output/*
	hdfs dfs -rmdir /user/$USER/$2/output
	rm *.jar *.class
elif [ $1 == "run" ];then
	hadoop com.sun.tools.javac.Main Log.java $3.java
	jar cf ws.jar *.class
	hadoop jar ws.jar $3 /user/$USER/$2/input /user/$USER/$2/output
elif [ $1 == "results" ];then
	hdfs dfs -cat /user/letz/$2/output/part-r-00000
fi

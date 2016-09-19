#!/bin/bash

if [ $# -ne 0 ]; then
    echo $0: "usage: ./run_spark.sh" 
    exit 1
fi

#input1=spark_read_me.txt
input1=stack/productivity/Posts.xml

$SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master local[2] \
    target/scala-2.11/spark-wc_2.11-1.0.jar $input1 

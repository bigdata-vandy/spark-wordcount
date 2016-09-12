#!/bin/bash

if [ $# -ne 0 ]; then
    echo $0: "usage: source run_spark.sh" 
    exit 1
fi

/bin/spark-submit \
    --class WordCountApp \
    --master local[2] \
    target/scala-2.11/spark-wc_2.11-1.0.jar \

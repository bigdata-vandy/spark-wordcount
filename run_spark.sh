#!/bin/bash

if [ $# -ne 0 ]; then
  echo $0: "usage: ./run_spark.sh input" 
  exit 1
fi

#input1=spark_read_me.txt
input1=stack/productivity/Posts.xml

PROGRAM="
    target/scala-2.11/spark-wc_2.11-1.0.jar \
    $input1
    "

export SPARK_HOME=/usr/lib/spark/


flag=0
if [ ${flag} == 0 ]
then
  # Run application locally
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master local[*] \
    $PROGRAM
elif [ ${flag} == 1 ]
then
# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
    $PROGRAM
elif [ ${flag} == 2 ]
then
  # Run on a YARN cluster
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master yarn \
    --deploy-mode client \
    --executor-memory 50m \
    --num-executors 6 \
    $PROGRAM
fi

#!/bin/bash

if [ $# -ne 0 ]; then
  echo $0: "usage: ./run_spark.sh input" 
  exit 1
fi

echo $SPARK_HOME

#input1=spark_read_me.txt
input1=stack/productivity/Posts.xml

APP="
    target/scala-2.11/spark-wc_2.11-1.0.jar \
    $input1
    "

flag=2
if [ ${flag} == 0 ]
then
  # Run application locally
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master local[*] \
    $APP
elif [ ${flag} == 1 ]
then
  # Run on a Spark standalone cluster in client deploy mode
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master spark://vmp741.vampire:7077 \
    $APP
elif [ ${flag} == 2 ]
then
  # Run on a YARN cluster
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 50m \
    --driver-cores 1 \
    --num-executors 2 \
    $APP
fi

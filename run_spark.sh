#!/bin/bash

if [ $# -ne 0 ]; then
  echo $0: "usage: ./run_spark.sh input" 
  exit 1
fi

echo $SPARK_HOME

#input1=spark_read_me.txt
#input1=stack/math.stackexchange.com/Posts.xml.gz
#input1=stack/stackoverflow.stackexchange.com/Comments.xml.gz
input1=stack/stackoverflow.stackexchange.com/Posts.xml.gz
output_dir="wordcount_$(date +%Y%m%d_%H%M%S)"


echo Reading input from $input1
echo Writing output to $output_dir

APP="
    target/scala-2.11/spark-wc_2.11-1.0.jar \
    $input1 \
    $output_dir
    "


flag=2
if [ ${flag} == 0 ]; then
  # Run application locally
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master local[*] \
    $APP
elif [ ${flag} == 1 ]; then
  # Run on a Spark standalone cluster in client deploy mode
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master spark://vmp741.vampire:7077 \
    $APP
elif [ ${flag} == 2 ]; then
  # Run on a YARN cluster
  $SPARK_HOME/bin/spark-submit \
    --class WordCountApp \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 25 \
    --executor-cores 3 \
    $APP
fi

# newsStreaming

To build execute:
```
sbt assembly
```

Example usage:
```
$SPARK_HOME/bin/spark-submit --class insightproject.spark.newsstreaming.NewsStreaming --master \
spark://`hostname`:7077 --jars newsStreaming-assembly-1.0.jar newsStreaming-assembly-1.0.jar \
my_topic my_group_id elastic_destination_index/elastic_doc_type
```

# newsStreaming
This package is part of my Insight Project [nexTop](https://github.com/rentzso/nextop).

The library runs a Spark Streaming consumer that receives messages posted by [the Kafka S3 Producer](https://github.com/rentzso/producerS3) and sends them into Elasticsearch using the native client library [elasticsearch-hadoop](https://github.com/elastic/elasticsearch-hadoop).

## Build and run instructions
To build the jar:
```
sbt assembly
```

Example usage in standalone:
```
$SPARK_HOME/bin/spark-submit --class insightproject.spark.newsstreaming.NewsStreaming --master \
spark://`hostname`:7077 --jars newsStreaming-assembly-1.0.jar newsStreaming-assembly-1.0.jar \
my_topic my_group_id elastic_destination_index/elastic_doc_type
```

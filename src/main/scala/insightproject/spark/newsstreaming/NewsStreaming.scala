package insightproject.spark.newsstreaming

/**
  * Created by rfrigato on 6/12/17.
  */
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark._

object NewsStreaming {
  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-10-0-0-8:9092,ip-10-0-0-11:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream_2",
      "auto.offset.reset" -> "earliest",//"latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("news_streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = Array("fromS3")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD { rdd =>
      rdd.map(record =>
        record.value()
      )
    }

    //val pairs = stream.map(record => ("count", 1))
    //val wordCounts = pairs.reduceByKey(_ + _)
    //stream.map(record => (record.key, record.value)).print()
    //wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

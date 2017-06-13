package insightproject.spark.newsstreaming

/**
  * Created by rfrigato on 6/12/17.
  */
import java.time.LocalTime

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
    sparkConf.set("es.nodes", "ip-10-0-0-4,ip-10-0-0-9,ip-10-0-0-10,ip-10-0-0-12")
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER"))
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = Array("fromS3")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD { rdd =>
      rdd.flatMap(record => {
        val line = record.value()
        val columns = line.split("\t")
        // Themes column
        if (columns.size < 8) {
          None
        }
        else if (columns(2) == "1") {
          val id = columns(0)
          val date = columns(1)
          val url = columns(4)
          val topics = columns(7).split(";")
          if (topics.size == 1 && topics(0) == ""){
            None
          }
          else {
            Option(Map(
              "id" -> id,
              "date" -> date,
              "url" -> url,
              "topics" -> topics,
              "timestamp" -> System.currentTimeMillis() / 1000l
            ))
          }
        } else {
          None
        }
      }).saveToEs("documents/news", Map("es.mapping.id" -> "id"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

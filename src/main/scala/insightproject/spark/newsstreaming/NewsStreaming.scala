package insightproject.spark.newsstreaming

/**
  * Created by rfrigato on 6/12/17.
  */
import java.time.LocalTime

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark._
import scala.collection.JavaConversions._

object NewsStreaming {
  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/parsed-gdelt-avro-schema.json")
    parser.parse(schemaFile)
  }
  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(gdeltAvroSchema)
  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-10-0-0-8:9092,ip-10-0-0-11:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "stream_2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("news_streaming")
    sparkConf.set("es.nodes", "ip-10-0-0-4,ip-10-0-0-9,ip-10-0-0-10,ip-10-0-0-12")
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER"))
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = Array("fromS3")
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )
    stream.foreachRDD { rdd =>
      rdd.map(record => {
        val bytes = record.value()
        convertAvroBytes(bytes)
      }).saveToEs("documents/news", Map("es.mapping.id" -> "id"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
  def convertAvroBytes(bytes:Array[Byte]) = {
    val record = recordInjection.invert(bytes).get
    val topics = record.get("topics")
                       .asInstanceOf[java.util.List[org.apache.avro.util.Utf8]]
                       .toArray.map(_.toString)
    Map(
      "id" -> record.get("id").toString,
      "date" -> record.get("date").toString,
      "url" -> record.get("url").toString,
      "topics" -> topics
    )
  }
}

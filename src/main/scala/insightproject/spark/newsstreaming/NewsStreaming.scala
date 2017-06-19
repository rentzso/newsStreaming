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
import org.apache.spark.streaming.kafka010.LocationStrategies.{PreferConsistent, PreferBrokers}
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
    if (args.length < 3) {
      throw new Exception("Missing parameter: topic, group id and elasticsearch destination are required")
    }
    val topic = args(0)
    val bootstrapServers: String = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> args(1),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("news_streaming")
    val esNodes: String = getEnvOrThrow("ES_NODES")
    sparkConf.set("es.nodes", esNodes)
    sparkConf.set("es.index.auto.create", "false")
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER"))
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS"))
    val timeWindow = sys.env.getOrElse("TIME_WINDOW", "5").toInt
    val ssc = new StreamingContext(sparkConf, Seconds(timeWindow))
    ssc.checkpoint(getEnvOrThrow("SPARK_CHECKPOINT_FOLDER"))
    val topics = Array(topic)
    val locationStrategy = sys.env.get("LOCATION_STRATEGY") match {
      case Some("PreferBrokers") => PreferBrokers
      case _ => PreferConsistent
    }
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      locationStrategy,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )
    val elasticDestination = args(2)
    stream.foreachRDD { rdd =>
      rdd.map(record => {
        val bytes = record.value()
        convertAvroBytes(bytes)
      }).saveToEs(elasticDestination, Map("es.mapping.id" -> "id"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
  private def getEnvOrThrow(envVariable: String): String = {
    sys.env.get(envVariable) match {
      case Some(value) => value
      case None => throw new Exception(s"Environment variable $envVariable not set")
    }
  }
  private def convertAvroBytes(bytes:Array[Byte]) = {
    val record = recordInjection.invert(bytes).get
    val topics = record.get("topics")
                       .asInstanceOf[java.util.List[org.apache.avro.util.Utf8]]
                       .toArray.map(_.toString)
    Map(
      "id" -> record.get("id").toString,
      "date" -> record.get("date").toString,
      "url" -> record.get("url").toString,
      "topics" -> topics,
      "num_topics" -> record.get("num_topics").asInstanceOf[Int]
    )
  }
}

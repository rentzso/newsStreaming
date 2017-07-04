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

/**
  * Creates a Kafka Direct Stream to read GDELT messages into Spark Streaming and send them to Elasticsearch
  */
object NewsStreaming {
  /**
    * Creates the Avro schema used to create the Avro record
    */
  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/parsed-gdelt-avro-schema.json")
    parser.parse(schemaFile)
  }
  /**
    * Converts an Avro record into Bytes
    */
  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(gdeltAvroSchema)

  /**
    * Creates a Spark Streaming context
    *
    * @param topic the topic we are reading from
    * @param groupId the Consumer group id the context is using
    * @param elasticDestination the "index/type" elasticsearch destination where the data is sent
    * @param checkpointDirectory the folder were the checkpoint is stored
    * @return
    */
  def getContext(topic: String, groupId: String, elasticDestination: String, checkpointDirectory: String): StreamingContext = {
    val bootstrapServers: String = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers, // Kafka bootstrap servers
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("news_streaming")
    val esNodes: String = getEnvOrThrow("ES_NODES")
    sparkConf.set("es.nodes", esNodes) // the elasticsearch nodes
    sparkConf.set("es.index.auto.create", "false") // do NOT create an index when it doesn't exist
    sparkConf.set("es.batch.write.refresh", "false") // do NOT refresh on write
    sparkConf.set("es.batch.size.entries", "1000") // size of the batch sent to elasticsearch
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER")) // elasticsearch user
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS")) // elasticsearch password
    val timeWindow = sys.env.getOrElse("TIME_WINDOW", "5").toInt
    val ssc = new StreamingContext(sparkConf, Seconds(timeWindow))
    ssc.checkpoint(checkpointDirectory)
    val topics = Array(topic)
    // Loads the location strategy from the ENV variable
    val locationStrategy = sys.env.get("LOCATION_STRATEGY") match {
      case Some("PreferBrokers") => PreferBrokers
      case _ => PreferConsistent
    }
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      locationStrategy,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.map(record => {
        val bytes = record.value()
        convertAvroBytes(bytes)
      }).saveToEs(elasticDestination, Map("es.mapping.id" -> "id"))
    }
    ssc
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new Exception("Missing parameter: topic, group id and elasticsearch destination are required")
    }
    val topic = args(0) // the topic we are reading from
    val groupId = args(1) // the consumer group id we are using
    val elasticDestination = args(2) // the "index/type" elasticsearch destination we are writing to
    val checkpointDirectory = getEnvOrThrow("SPARK_CHECKPOINT_FOLDER")
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => getContext(topic, groupId, elasticDestination, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Gets the variable or throws an exception
    *
    * @param envVariable
    * @return the value of the environment variable
    */
  private def getEnvOrThrow(envVariable: String): String = {
    sys.env.get(envVariable) match {
      case Some(value) => value
      case None => throw new Exception(s"Environment variable $envVariable not set")
    }
  }

  /**
    * Parses a binary Avro message into a Map
    *
    * @param bytes
    * @return the Map resulting from the Avro message
    */
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

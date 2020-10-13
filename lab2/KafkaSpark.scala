package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {

    // connect to Cassandra and make a keyspace and table as explained in the document

    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")


    // make a connection to Kafka and read (key, value) pairs from it
    // connect Spark to Kafka in the receiver-less direct approach where Spark periodically queries Kafka for the latest offsets in each topic + partition

    val sparkConf = new SparkConf().setAppName("KafkaSparkAverageValue").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    ssc.checkpoint(".")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, Set("avg"))

    val allValues = kafkaStream.map(_._2)

    val splitEach = allValues.map(_.split(","))

    val keyValue = splitEach.map(eachPair => (eachPair(0), eachPair(1).toDouble))

    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double, Double)]): (String, Double) = {
      val newData = value.getOrElse(0.0);
      var (count, sum, average) = state.getOption.getOrElse((0, 0.0, 0.0))

      count = count + 1;
      sum = sum + newData;
      average = sum / count;

      state.update((count, sum, average))
      (key, average)
    }

    val stateDstream = keyValue.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
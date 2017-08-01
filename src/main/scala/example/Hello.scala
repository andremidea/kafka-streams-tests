package example

import java.lang.Long
import java.sql.Timestamp
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde

import scala.collection.mutable
import scala.jav

class BatchInitializer extends Initializer[Batch] {
  override def apply(): Batch = new Batch()
}

class Batch {
  val foo = mutable.ArrayBuffer[String]()

  def add(s: String) = {
    foo += s
    this
  }
}
object Hello extends Greeting with App {
  Foo.euclidesLikesHillary

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  val i1: Initializer[String] =  () => ""
  val ag1: Aggregator[String, String, String] = (k, v, va) => va + v
  val kv: KeyValueMapper[Windowed[String], String, String] = (k, v) => k.window().start()


  val builder: KStreamBuilder = new KStreamBuilder()
  val aggregate: KTable[Windowed[String], String] = builder.stream("teste1")
    .groupByKey()
    .aggregate(i1,
      ag1,
      TimeWindows.of(5000),
      new StringSerde(), "foo")

  aggregate.toStream((x, v) => )



//  val wordCounts: KTable[String, Long] = textLines
//    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
//    .groupBy((_, word) => word)
//    .count("Counts")
//  wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic")

  val streams: KafkaStreams = new KafkaStreams(builder, config)
  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))
}

object Foo {
  def euclidesLikesHillary = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")

    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    2
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)

    Range(0, 1000)
      .partition(_ % 2 == 0) match {
      case (a, b) => {
        val y = (x: Int) => Instant.now().toEpochMilli + (x * 200)
        a.foreach(x => producer.send(new ProducerRecord("teste1", x % 2, y(x), "value", x.toString)))
        b.foreach(x => producer.send(new ProducerRecord("teste1", x % 2, y(x), "value", x.toString)))
      }
    }
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}

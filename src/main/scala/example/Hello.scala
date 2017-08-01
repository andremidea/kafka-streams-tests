package example

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde

import scala.collection.mutable

class BatchInitializer extends Initializer[Batch] {
  override def apply(): Batch = new Batch()
}

class Batch {
  private val _foo: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()

  val foo: String = _foo.reduce(_ + _)

  def add(s: String): Batch = {
    _foo += s
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

  val i1: Initializer[String]                                     = () => ""
  val aggregator: Aggregator[String, String, String]              = (k, v, va) => va.concat(" | ").concat(v)
  val keyMapper: KeyValueMapper[Windowed[String], String, String] = (k, v) => k.key() // we don't want to change the key
  val valueMapper: ValueMapper[String, String]                    = (v) => v

  val builder: KStreamBuilder               = new KStreamBuilder()
  val streamEvents: KStream[String, String] = builder.stream("stream-events")
  val aggregate: KTable[Windowed[String], String] = streamEvents
    .groupByKey()
    .aggregate(i1, aggregator, TimeWindows.of(5000), new StringSerde(), "foo")

  aggregate.to("stream-events-agg")

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

    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)

    Range(0, 1000)
      .partition(_ % 2 == 0) match {
      case (a, b) =>
        val y = (x: Int) => Instant.now().toEpochMilli + (x * 200)
        a.foreach(x => producer.send(new ProducerRecord("stream-events", 0, y(x), "value", x.toString.concat("-").concat(y.toString))))
        b.foreach(x => producer.send(new ProducerRecord("stream-events", 0, y(x), "value", x.toString.concat("-").concat(y.toString))))
    }
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}

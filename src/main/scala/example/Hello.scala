package example

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
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

class Foo extends Processor[String, String] {

  var processor: ProcessorContext = null
  var state: KeyValueStore[String, String] = null
  val k = "chupa_conrado2"

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(10000)
    state = context.getStateStore("FOO").asInstanceOf[KeyValueStore[String, String]]
  }

  override def process(key: String, value: String): Unit = {
    val s: String = state.get(k)
    println(s)

    if(s != null) {
      println("not null")
      state.put(k, s + " | " + value)
    } else {
      state.put(k, ">>>>>> " + value)
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    val s = state.get(k)

    if (s != null) {
      print(s.size)
      if(s.size > 1000) {
        state.delete(k)
        processor.forward(k, s)
        processor.commit()
      }
    }
  }

  override def close(): Unit = {}
}

object Hello extends Greeting with App {
  val name = "app03"
  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  val proc: ProcessorSupplier[String, String] = () => new Foo()

  val store = Stores.create("FOO")
    .withKeys(Serdes.String())
    .withValues(Serdes.String())
    .persistent()
    .build()

  val builder2: TopologyBuilder = new TopologyBuilder()
  builder2
    .addSource("SOURCE", "stream-events")
    .addProcessor("AGGREGATE", proc, "SOURCE")
    .addSink("SINK", "stream-events-agg", "AGGREGATE")
    .addStateStore(store, "AGGREGATE")
    .connectProcessorAndStateStores("AGGREGATE", "FOO")



//  val i1: Initializer[String]                                     = () => s"XXX>>>>>"
//  val aggregator: Aggregator[String, String, String]              = (k, v, va) => va.concat(" | ").concat(v)
//  val keyMapper: KeyValueMapper[Windowed[String], String, String] = (k, v) => k.key() // we don't want to change the key
//  val valueMapper: ValueMapper[String, String]                    = (v) => v
//
//  val serdeString: Serde[String] = Serdes.String()
//
//  val builder: KStreamBuilder               = new KStreamBuilder()
//  val streamEvents: KStream[String, String] = builder.stream("stream-events")
//  val aggregate: KTable[Windowed[String], String] = streamEvents
//    .groupByKey()
//    .aggregate(i1, aggregator, TimeWindows.of(30000), serdeString, "foo")
//
//
//  val stream: KStream[String, String] = aggregate.toStream(keyMapper)
//
//  stream.to(Serdes.String(), Serdes.String(), "stream-events-agg2")

//  val wordCounts: KTable[String, Long] = textLines
//    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
//    .groupBy((_, word) => word)
//    .count("Counts")
//  wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic"),

//  aggregate.to(serdeString, serdeString, "stream-events-agg")

  val streams: KafkaStreams = new KafkaStreams(builder2, config)

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
    def round[A](seq: Iterable[A], n: Int) = {
      (0 until n).map(i => seq.drop(i).sliding(1, n).flatten)
    }
    round(Stream.from(1), 1000).foreach { x =>
      x
        .partition(_ % 2 == 0) match {
        case (a, b) =>
          val y = (x: Int) => Instant.now().toEpochMilli + (x * 2000)
          a.foreach(x => producer.send(new ProducerRecord("stream-events", x % 2, y(x), "value", x.toString.concat("-").concat(y(x).toString))))
          b.foreach(x => producer.send(new ProducerRecord("stream-events", x % 2, y(x), "value", x.toString.concat("-").concat(y(x).toString))))
      }
      Thread.sleep(5000)
    }
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}

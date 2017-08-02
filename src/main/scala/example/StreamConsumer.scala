package example

import java.time.Instant
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBuilderBase
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

case class Message[M, T](metadata: M, payload: T)

object Types {
  type Message = String
  type KVStoreType = KeyValueStore[String, Message]
}

class StreamProcessor extends Processor[String, Types.Message] {

  private var processor: ProcessorContext = _
  private var state: Types.KVStoreType = _

  private var lastFlush: Long = Instant.now().toEpochMilli

  val MAX_FLUSH_LAG: Long = 60 // seconds
  val MAX_FLUSH_MESSAGES: Int = 100 // messages

  def setLastFlush(): Unit = {
    lastFlush.synchronized {
      lastFlush = Instant.now().toEpochMilli
    }
  }

  def flushLagSeconds(): Long = {
    (Instant.now().toEpochMilli - lastFlush) / 1000
  }

  val stateKey: String = "messages"

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(10000)
    state = context.getStateStore("FOO").asInstanceOf[Types.KVStoreType]
  }

  override def process(key: String, value: Types.Message): Unit = {
    state.put(key, value)
  }

  private def flush(): Unit = {
    val x = state.all()
    val messages = x.asScala.toSeq.map(x => AvroThings.Fufuzer(x.key))
    val y = AvroThings.getBAOS(messages)

    processor.forward(Random.nextInt().toString, y)
    setLastFlush()
    processor.commit()
    x.asScala.foreach(x => state.delete(x.key))
  }

  override def punctuate(timestamp: Long): Unit = {
    if (state.approximateNumEntries() > MAX_FLUSH_MESSAGES) {
      println("Flushing for SIZE")
      flush()
    }
    else if (flushLagSeconds > MAX_FLUSH_LAG) {
      println("Flushing for TIME")
      flush()
    }
  }


  override def close(): Unit = {}
}



object StreamConsumer extends App {

  val name = "app01"

  val TO_TOPIC: String = "stream-events-agg"

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  val proc: ProcessorSupplier[String, Types.Message] = () => new StreamProcessor()
  val writter: ProcessorSupplier[String, Array[Byte]] = () => new StreamWriter()

  val store = Stores.create("FOO")
    .withKeys(Serdes.String())
    .withValues(Serdes.String())
    .persistent()
    .build()

  val builder: TopologyBuilder = new TopologyBuilder()
  builder
    .addSource("SOURCE", "stream-events")
    .addProcessor("AGGREGATE", proc, "SOURCE")
    .addProcessor("WRITER", writter, "AGGREGATE")
    .addStateStore(store, "AGGREGATE")
    .connectProcessorAndStateStores("AGGREGATE", "FOO")

  val streams: KafkaStreams = new KafkaStreams(builder, config)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))

  println("shutdown")

}

object Producer {

  val TO_TOPIC: String = "stream-events"

  def getTimestamp(x: Int): Long = Instant.now().toEpochMilli + (x * 2000)

  def getValue(x: Int): String = {
    x.toString
  }

  def produce(): Unit = {

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")

    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)

    Range(0, 1000).foreach { r =>
      Range(r*20, (r+1)*20)
        .partition(_ % 2 == 0) match {
        case (a, b) =>
          a.foreach(x => producer.send(new ProducerRecord(TO_TOPIC, x % 2, getTimestamp(x), getTimestamp(x).toString, getValue(x))))
          b.foreach(x => producer.send(new ProducerRecord(TO_TOPIC, x % 2, getTimestamp(x), getTimestamp(x).toString, getValue(x))))
      }
      println("Sleeping")
      Thread.sleep(5000)
    }
  }
}

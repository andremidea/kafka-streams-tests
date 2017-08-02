package example

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

case class Message[M, T](metadata: M, payload: T)

object Types {
  type MessageString = String
  type Messages = String
  type KVStoreType = KeyValueStore[String, Messages]
}

class StreamProcessor extends Processor[String, Types.MessageString] {

  private var processor: ProcessorContext = _
  private var state: Types.KVStoreType = _

  private var lastFlush: Long = Instant.now().toEpochMilli

  val MAX_FLUSH_LAG: Long = 30 // seconds
  val MAX_FLUSH_MESSAGES: Int = 30 // messages
  val Initializer: Types.MessageString = ">>>> "

  def appender(currentState: Types.Messages, value: Types.MessageString): Types.Messages = currentState.concat(" | ").concat(value)

  def setLastFlush(): Unit = {
    lastFlush.synchronized {
      lastFlush = Instant.now().toEpochMilli
    }
  }

  def flushLagSeconds(): Long = {
    (Instant.now().toEpochMilli - lastFlush)/1000
  }

  val stateKey: String = "messages"

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(10000)
    state = context.getStateStore("FOO").asInstanceOf[Types.KVStoreType]
  }

  override def process(key: String, value: Types.MessageString): Unit = {
    val currentMessages: Types.Messages = state.get(stateKey)
    println(currentMessages)

    if(currentMessages != null) {
      state.put(stateKey, appender(currentMessages, value))
    } else {
      state.put(stateKey, appender(Initializer, value))
    }
  }

  private def flush(currentMessages: Types.Messages): Unit = {
    processor.forward(stateKey, currentMessages)
    state.delete(stateKey)
    setLastFlush()
    processor.commit()
  }

  override def punctuate(timestamp: Long): Unit = {
    val currentMessages: Types.Messages = state.get(stateKey)

    if (currentMessages != null) {
      print(currentMessages.size)
      if(currentMessages.size > MAX_FLUSH_MESSAGES) {
        println("Flushing for SIZE")
        flush(currentMessages)
      }
      else if (flushLagSeconds > MAX_FLUSH_LAG) {
        println("Flushing for TIME")
        flush(currentMessages)
      }
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

  val proc: ProcessorSupplier[String, Types.MessageString] = () => new StreamProcessor()

  val store = Stores.create("FOO")
    .withKeys(Serdes.String())
    .withValues(Serdes.String())
    .persistent()
    .build()

  val builder: TopologyBuilder = new TopologyBuilder()
  builder
    .addSource("SOURCE", "stream-events")
    .addProcessor("AGGREGATE", proc, "SOURCE")
    .addSink("SINK", TO_TOPIC, "AGGREGATE")
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
    val timestamp = Instant.ofEpochMilli(getTimestamp(x)).toString
    val value = x.toString
    timestamp.concat(": ").concat(value)
  }

  def produce(): Unit = {

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
          println("case")
          a.foreach(x => producer.send(new ProducerRecord(TO_TOPIC, x % 2, getTimestamp(x), "key", getValue(x))))
          println("case b")
          b.foreach(x => producer.send(new ProducerRecord(TO_TOPIC, x % 2, getTimestamp(x), "key", getValue(x))))
      }
      println("Sleeping")
      Thread.sleep(5000)
    }
  }
}

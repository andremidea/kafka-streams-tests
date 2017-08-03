package kafka.streams

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.Stores

import scala.collection.JavaConverters._
import scala.math.random

class StreamProcessor extends Processor[String, Array[Byte]] {

  private var processor: ProcessorContext = _
  private var state: KVStoreType          = _

  private var lastFlush: Long = Instant.now().toEpochMilli

  val MAX_FLUSH_LAG: Long     = 5  // seconds
  val MAX_FLUSH_MESSAGES: Int = 20 // messages

  def setLastFlush(): Unit = {
    lastFlush.synchronized {
      lastFlush = Instant.now().toEpochMilli
    }
  }

  def flushLagSeconds(): Long = {
    (Instant.now().toEpochMilli - lastFlush) / 1000
  }

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(10000)
    state = context.getStateStore("STORE").asInstanceOf[KVStoreType]
  }

  override def process(key: String, value: Array[Byte]): Unit = {
    state.put(key, value)
  }

  private def flush(): Unit = {
    val batchId  = s"B:${(random * 4000).toInt}"
    val allState = state.all().asScala.toSeq
    println(s"allState ${allState.size}")
    val messages: Seq[OutputMessage] =
      allState.flatMap(x =>
        AvroThings.getResultFromBA[InputMessage](x.value).map(value => OutputMessage(batchId, value)))
    val baos = AvroThings.getBAOS(messages)

    processor.forward(batchId, baos)
    setLastFlush()
    processor.commit()
    allState.foreach(x => state.delete(x.key))
  }

  override def punctuate(timestamp: Long): Unit = {
    if (state.approximateNumEntries() > MAX_FLUSH_MESSAGES) {
      println("Flushing for SIZE")
      flush()
    } else if (flushLagSeconds > MAX_FLUSH_LAG && state.approximateNumEntries() > 0) {
      println("Flushing for TIME")
      flush()
    }
  }

  override def close(): Unit = {}
}

class StreamConsumer {

  val name = "app-stream-02"

  val FROM_TOPIC: String = StreamProducer.TO_TOPIC
  val TO_TOPIC: String   = "stream-events-agg"

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass)
    p
  }

  val proc: ProcessorSupplier[String, Array[Byte]]    = () => new StreamProcessor()
  val writter: ProcessorSupplier[String, Array[Byte]] = () => new StreamWriter()

  private val store = Stores
    .create("STORE")
    .withStringKeys()
    .withByteArrayValues()
    .persistent()
    .build()

  val builder: TopologyBuilder = new TopologyBuilder()
  builder
    .addSource("SOURCE", FROM_TOPIC)
    .addProcessor("PROCESSOR", proc, "SOURCE")
    .addProcessor("WRITER", writter, "PROCESSOR")
    .addSink("SINK", TO_TOPIC, Serdes.String().serializer(), Serdes.ByteArray().serializer(), "PROCESSOR")
    .addStateStore(store, "PROCESSOR")
    .connectProcessorAndStateStores("PROCESSOR", "STORE")

  val streams: KafkaStreams = new KafkaStreams(builder, config)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))

  println("shutdown")

}

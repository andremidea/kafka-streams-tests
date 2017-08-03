package kafka.streams

import java.time.Instant
import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.math.random

object StreamProducer {

  val startTime: Long = Instant.now().toEpochMilli

  val TO_TOPIC: String = "stream-events"

  def getTimestamp(x: Int): Long = Instant.now().toEpochMilli + (x * 200)

  def getValue(x: Int): String = {
    x.toString
  }

  def getRecord(x: Int): (ProducerRecord[String, Array[Byte]], InputMessage) = {
    val ts           = getTimestamp(x)
    val k            = UUID.randomUUID().toString
    val inputMessage = InputMessage(k, getValue(x), s"P:${x % 2}", ts)
    val v            = AvroThings.getBAOS(Seq(inputMessage))
    val record       = new ProducerRecord(TO_TOPIC, x % 2, ts, k, v)
    (record, inputMessage)
  }

  def produce(): Unit = {

    val kafkaProps = new Properties()

    kafkaProps.put("bootstrap.servers", "localhost:9092")

    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)

    Range(0, 1000).foreach { r =>
      Range(r * 12, (r + 1) * 12)
        .partition(_ % 2 == 0) match {
        case (a, b) =>
          val records = a.map(x => getRecord(x)) ++ b.map(x => getRecord(x))
          records.foreach(x => producer.send(x._1))
          val inputMessages: Seq[InputMessage] = records.map(_._2)
          val baos = AvroThings.getBAOS(inputMessages)
          AvroThings.writeAvroInputMessage(baos, "input")
      }
      val sleepTime: Int = (random * 4000).toInt
      println(s"Sleeping $sleepTime")
      Thread.sleep(sleepTime)
    }
  }
}

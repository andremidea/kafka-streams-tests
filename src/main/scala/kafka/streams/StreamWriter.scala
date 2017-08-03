package kafka.streams

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

class StreamWriter extends Processor[String, Array[Byte]] {


  private var processor: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(10000)
  }

  override def process(key: String, value: Array[Byte]): Unit = {
    AvroThings.printBytesOutputMessage(value)
    AvroThings.writeAvroOutputMessage(value, "output")
    processor.commit()

  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}
}

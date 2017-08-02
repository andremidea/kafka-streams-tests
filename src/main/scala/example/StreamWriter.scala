package example

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

/**
  * Created by andre on 02/08/17.
  */
class StreamWriter extends Processor[String, Array[Byte]] {

  override def init(context: ProcessorContext): Unit = { }

  override def process(key: String, value: Array[Byte]): Unit = {
    AvroThings.printbytes(value)
  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}
}

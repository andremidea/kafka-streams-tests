package kafka

import java.time.Instant

import org.apache.kafka.streams.state.KeyValueStore

package object streams {

  case class InputMessage(key: String, value: String, partition: String, producedAt: Long)
  case class OutputMessage(batchId: String, inputMessage: InputMessage, consumedAt: Long = Instant.now().toEpochMilli)

  type KVStoreType  = KeyValueStore[String, Array[Byte]]

}

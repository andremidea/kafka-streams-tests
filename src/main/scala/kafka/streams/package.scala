package kafka

import java.time.Instant

import org.apache.kafka.streams.state.KeyValueStore

package object streams {

  case class Message[K, T](key: K, payload: T, consumedAt: Long = Instant.now().toEpochMilli)
  type OutputMessage = Message[String, InputMessage]

  type InputMessage = Message[Long, String]
  type KVStoreType  = KeyValueStore[String, Array[Byte]]

}

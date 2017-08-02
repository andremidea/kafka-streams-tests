package kafka.streams

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, SchemaFor, ToRecord, FromRecord}

object AvroThings {

  def getBAOS[T: SchemaFor: ToRecord](input: Seq[T]): Array[Byte] = {
    val baos   = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[T](baos)
    output.write(input)
    output.close()
    val bytes = baos.toByteArray
    bytes
  }

  def getResultFromBA[T: SchemaFor: FromRecord](bytes: Array[Byte]): Iterator[T] = {
    val in     = new ByteArrayInputStream(bytes)
    val input  = AvroInputStream.binary[T](in)
    val result = input.iterator
    result
  }

  def printBytes[T: SchemaFor: FromRecord](bytes: Array[Byte]): Unit = {
    val result = getResultFromBA(bytes).toSeq
    println(s"result.size: ${result.size}")
    result.foreach(x => print(s"$x "))
  }

  //def getBAOSInputMessage: (Seq[InputMessage]) => Array[Byte] = getBAOS[InputMessage]
  //def getBAOSOutputMessage: (Seq[OutputMessage]) => Array[Byte] = getBAOS[OutputMessage]

  //def getResultFromBA: (Array[Byte]) => Unit = getResultFromBA[InputMessage]
  def printBytesOutputMessage: (Array[Byte]) => Unit = printBytes[OutputMessage]

}

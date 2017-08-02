package example

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}

/**
  * Created by andre on 02/08/17.
  */
object AvroThings {

  case class Fufuzer(value: Types.Message)
  val schema = AvroSchema[Fufuzer]


  def getBAOS(xxx: Seq[Fufuzer]) = {
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[Fufuzer](baos)
    output.write(xxx)
    output.close()
    val bytes = baos.toByteArray
    bytes
  }

  def printbytes(bytes: Array[Byte]) = {
    val in = new ByteArrayInputStream(bytes)
    val input = AvroInputStream.binary[Fufuzer](in)
    val result = input.iterator.toSeq

    result.foreach(x=> print(s"${x} "))
  }

}

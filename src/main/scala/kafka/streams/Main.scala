package kafka.streams

object Main extends App {
  override def main(args: Array[String]): Unit = {
    args match {
      case Array("produce") => StreamProducer.produce()
      case Array("consume") => new StreamConsumer()
      case _ => println("Foo")
    }
    super.main(args)

  }
}

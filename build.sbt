import Dependencies._


resolvers ++= Seq(
  Resolver.mavenLocal,
  "clojars" at "http://clojars.org/repo",
  "confluent" at "http://packages.confluent.io/maven/")

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "StreamConsumer",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.1",
    libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.3.0",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1",
    libraryDependencies += "org.apache.avro" % "avro" % "1.8.2",
    libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.7.0"
  )

//    <repositories>
//        <repository>
//            <id>confluent</id>
//            <url>http://packages.confluent.io/maven/</url>
//        </repository>
//    </repositories>

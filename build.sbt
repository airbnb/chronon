import sbt.Keys._

ThisBuild / organization := "ai.zipline"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .aggregate(api, aggregator, spark)
  .settings(
    name := "zipline"
  )
import scala.sys.process._

lazy val api = project
  .settings(
    sourceGenerators in Compile += Def.task {
      val inputThrift = baseDirectory.value / "src" / "main" / "thrift" / "api.thrift"
      val outputThrift = (Compile / sourceManaged).value
      s"rm -rf ${outputThrift.getPath}" !;
      s"mkdir -p ${outputThrift.getPath}" !;
      // "thrift --gen java -out api/src/main/java api/src/main/thrift/api.thrift"
      s"thrift --gen java -out ${outputThrift.getPath} ${inputThrift.getPath}" !;
      (outputThrift ** "*.scala").get()
    }.taskValue,
    libraryDependencies ++= Seq(
      // lower version due to jackson conflict with spark
      "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.0",
      "org.apache.thrift" % "libthrift" % "0.13.0"
    )
  )

lazy val aggregator = project
  .dependsOn(api)
  .settings(
    libraryDependencies ++= Seq(
      "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "com.google.code.gson" % "gson" % "2.8.6"
    )
  )

lazy val spark = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .settings(
    mainClass in (Compile, run) := Some("ai.zipline.spark.Join"),
    // assemblySettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.4.4",
      "org.rogach" %% "scallop" % "4.0.1"
    )
  )

// TODO add benchmarks - follow this example
// https://github.com/sksamuel/avro4s/commit/781aa424f4affc2b8dfa35280c583442960df08b

import sbt.Keys._

import scala.reflect.io.Path

ThisBuild / organization := "ai.zipline"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .aggregate(api, aggregator, spark)
  .settings(name := "zipline")

lazy val api = project
  .settings(
    sourceGenerators in Compile += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    libraryDependencies ++= Seq("org.apache.thrift" % "libthrift" % "0.13.0")
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

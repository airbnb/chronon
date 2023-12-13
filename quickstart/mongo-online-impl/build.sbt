import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ai.chronon"
ThisBuild / organizationName := "Chronon"

lazy val root = (project in file("."))
  .settings(
    name := "mongo-online-impl",
    libraryDependencies ++= Seq(
      "ai.chronon" %% "api" % "0.0.57",
      "ai.chronon" %% "online" % "0.0.57" % Provided,
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1", // Batch upload + structured streaming
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.8.1",    // Fetching
          "ch.qos.logback" % "logback-classic" % "1.2.3",
          "org.slf4j" % "slf4j-api" % "1.7.32"
    ),
  )

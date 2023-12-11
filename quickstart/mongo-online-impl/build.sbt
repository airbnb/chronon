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
      "ai.chronon" %% "online" % "0.0.57" % Provided,
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1", // Batch upload + structured streaming
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.0" % Provided,    // Fetching
    ),
  )

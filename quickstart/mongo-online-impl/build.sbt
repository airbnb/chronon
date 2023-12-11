import Dependencies._

ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "mongo-online-impl",
    libraryDependencies ++= Seq(
      "ai.chronon" %% "online" % "0.0.57", // Replace with the latest version if needed
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1", // Batch upload + structured streaming
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.4.1"      // Fetching
    ),
  )


import sbt.Keys.{libraryDependencies, *}
import sbt.Test
import sbt.*

// Notes about a few dependencies - and how we land on versions
// Our approach is to use the latest stable versions of deps as of today (July 24) and pin to them for a few years
// this should simplify our build setup, speed up CI and deployment

// latest dataproc and emr versions at the time (July 2024) of writing this comment are 2.2.x and 7.1.0 respectively
// google dataproc versions 2.2.x: https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2
// flink is at 1.17.0, spark is at 3.5.0, scala is at 2.12.17, Java 11

// emr 7.1.0 versions:https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-710-release.html
// spark is at 3.5.0, flink is at 1.18.1, scala is at 2.12.18, Java 17

// java incompatibility is probably not an issue, hopefully we can cross build flink 1.17 & 1.18 without code changes

lazy val scala_2_12 = "2.12.18"
// spark deps: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.5.0
// avro 1.11.2, jackson: 2.15.2
lazy val spark_3_5 = "3.5.1"
// flink deps: https://mvnrepository.com/artifact/org.apache.flink/flink-java/1.17.1
// jackson is shaded 2.13-2.16, no avro dependency
lazy val flink_1_18 = "1.18.1"
lazy val jackson_2_15 = "2.15.2"
lazy val avro_1_11 = "1.11.2"

// skip tests on assembly - uncomment if builds become slow
// ThisBuild / assembly / test := {}

ThisBuild / scalaVersion := scala_2_12

lazy val supportedVersions = List(scala_2_12) // List(scala211, scala212, scala213)

lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark_uber, flink)
  .settings(name := "chronon")

/**
  * Versions are look up from mvn central - so we are fitting for three configurations
  * scala 11 + spark 2.4: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11/2.4.0
  * scala 12 + spark 3.1.1: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.1.1
  * scala 13 + spark 3.2.1: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.13/3.2.1
  */

val spark_sql = Seq(
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-core"
).map(_ % spark_3_5)
val spark_sql_provided = spark_sql.map(_ % "provided")

val spark_all = Seq(
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-hive",
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-sql-kafka-0-10"
).map(_ % spark_3_5)
val spark_all_provided = spark_all.map(_ % "provided")

val jackson = Seq(
  "com.fasterxml.jackson.core" % "jackson-core",
  "com.fasterxml.jackson.core" % "jackson-databind",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"
).map(_ % jackson_2_15)

val flink_all = Seq(
  "org.apache.flink" %% "flink-streaming-scala",
  "org.apache.flink" % "flink-metrics-dropwizard",
  "org.apache.flink" % "flink-clients",
  "org.apache.flink" % "flink-test-utils"
).map(_ % flink_1_18)

val avro = Seq("org.apache.avro" % "avro" % "1.11.3")

lazy val api = project
  .settings(
    Compile / sourceGenerators += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= spark_sql_provided,
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.20.0",
      "javax.annotation" % "javax.annotation-api" % "1.3.2",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % "test"
    )
  )

lazy val aggregator = project
  .dependsOn(api.%("compile->compile;test->test"))
  .settings(
    libraryDependencies ++= Seq(
        "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
        "com.google.code.gson" % "gson" % "2.10.1"
      ),
    libraryDependencies ++= spark_sql_provided,
  )

lazy val online = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
      "com.datadoghq" % "java-dogstatsd-client" % "4.4.1",
      "org.rogach" %% "scallop" % "5.1.0",
      "net.jodah" % "typetools" % "0.6.3",
      "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
    ),
    libraryDependencies ++= spark_all,
  )

// TODO: see if we can unify online shaded & un shaded
// in theory only flink needs the online package,
lazy val online_unshaded = (project in file("online"))
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    target := target.value.toPath.resolveSibling("target-no-assembly").toFile,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
      "com.datadoghq" % "java-dogstatsd-client" % "4.4.1",
      "org.rogach" %% "scallop" % "5.1.0",
      "net.jodah" % "typetools" % "0.6.3",
      "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
    ),
    libraryDependencies ++= jackson,
    libraryDependencies ++= spark_all.map(_ % "provided"),
  )


lazy val tmp_warehouse = "/tmp/chronon/"
def cleanSparkMeta(): Unit = {
  Folder.clean(file(".") / "spark" / "spark-warehouse",
               file(tmp_warehouse) / "spark-warehouse",
               file(".") / "spark" / "metastore_db",
               file(tmp_warehouse) / "metastore_db")
}

val sparkBaseSettings: Seq[Setting[_]] = Seq(
  assembly / test := {},
  assembly / artifact := {
    val art = (assembly / artifact).value
    art.withClassifier(Some("assembly"))
  },
  mainClass in (Compile, run) := Some("ai.chronon.spark.Driver"),
  cleanFiles ++= Seq(file(tmp_warehouse)),
  Test / testOptions += Tests.Setup(() => cleanSparkMeta()),
  // compatibility for m1 chip laptop
  libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.10.4" % Test
) ++ addArtifact(assembly / artifact, assembly)

lazy val spark_uber = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online_unshaded)
  .settings(
    sparkBaseSettings,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= spark_all_provided,
  )

lazy val flink = (project in file("flink"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    libraryDependencies ++= spark_all,
    libraryDependencies ++= flink_all,
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _ @_*)         => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case PathList("com", "fasterxml", _ @_*) => MergeStrategy.last
  case PathList("com", "google", _ @_*)    => MergeStrategy.last
  case _                                   => MergeStrategy.first
}
exportJars := true

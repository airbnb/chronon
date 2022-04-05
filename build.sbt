import sbt.Keys._
import sbt.Test

ThisBuild / organization := "ai.zipline"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := Option(System.getProperty("version")).getOrElse("local")

lazy val publishSettings = Seq(
  publishTo := Some("Artifactory Realm" at "https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases"),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
)

lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark_uber, spark_embedded)
  .settings(
    publish / skip := true,
    crossScalaVersions := Nil,
    name := "zipline"
  )

lazy val api = project
  .settings(
    publishSettings,
    sourceGenerators in Compile += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    crossScalaVersions := List("2.11.12", "2.13.6"),
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.13.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.10",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.10",
      "org.scala-lang" % "scala-reflect" % "2.11.12",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.9.10",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0"
    ),
    unmanagedSourceDirectories in Compile ++= {
      (unmanagedSourceDirectories in Compile).value.map { dir =>
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, 13)) => file(dir.getPath ++ "213")
          case _             => file(dir.getPath ++ "211")
        }
      }
    }
  )

lazy val aggregator = project
  .dependsOn(api)
  .settings(
    publishSettings,
    crossScalaVersions := List("2.11.12", "2.13.6"),
    libraryDependencies ++= Seq(
      "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "com.google.code.gson" % "gson" % "2.8.6"
    )
  )

lazy val online = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .settings(
    publishSettings,
    crossScalaVersions := List("2.11.12", "2.13.6"),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      "com.datadoghq" % "java-dogstatsd-client" % "2.7",
      "org.rogach" %% "scallop" % "4.0.1",
      "org.apache.avro" % "avro" % "1.8.0",
      "net.jodah" % "typetools" % "0.4.1"
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, major)) if major <= 12 =>
          Seq()
        case _ =>
          Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
      }
    }
  )

def cleanSparkMeta(): Unit = {
  Folder.clean(file(".") / "spark" / "spark-warehouse",
               file(".") / "spark-warehouse",
               file(".") / "spark" / "metastore_db",
               file(".") / "metastore_db")
}

val sparkLibs = Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"
)

val sparkBaseSettings: Seq[Def.SettingsDefinition] = Seq(
  assembly / test := {},
  assembly / artifact := {
    val art = (assembly / artifact).value
    art.withClassifier(Some("assembly"))
  },
  addArtifact(assembly / artifact, assembly),
  publishSettings,
  mainClass in (Compile, run) := Some("ai.zipline.spark.Driver"),
  cleanFiles ++= Seq(
    baseDirectory.value / "spark-warehouse",
    baseDirectory.value / "metastore_db"
  ),
  testOptions in Test += Tests.Setup(() => cleanSparkMeta()),
  testOptions in Test += Tests.Cleanup(() => cleanSparkMeta())
)

val providedLibs: Setting[_] = libraryDependencies ++= sparkLibs.map(_ % "provided")
val embeddedLibs: Setting[_] = libraryDependencies ++= sparkLibs
val embeddedTarget: Setting[_] = target := target.value.toPath.resolveSibling("target-embedded").toFile
val embeddedAssemblyStrategy: Setting[_] = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _ @_*)         => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case _                                   => MergeStrategy.first
}
val sparkProvided = sparkBaseSettings :+ providedLibs
val sparkEmbedded =
  sparkBaseSettings :+ embeddedLibs :+ embeddedTarget :+ embeddedAssemblyStrategy :+ (Test / test := {})

lazy val spark_uber = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(sparkProvided: _*)

// Project for running with embedded spark for local testing
lazy val spark_embedded = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(sparkEmbedded: _*)

exportJars := true

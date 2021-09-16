import sbt.Keys._

ThisBuild / organization := "ai.zipline"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1-SNAPSHOT"

lazy val root = (project in file("."))
  .aggregate(api, aggregator, spark, fetcher)
  .settings(name := "zipline")

lazy val api = project
  .settings(
    sourceGenerators in Compile += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.13.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
    )
  )

lazy val aggregator = project
  .dependsOn(api)
  .settings(
    assembly / test := {},
    libraryDependencies ++= Seq(
      "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "com.google.code.gson" % "gson" % "2.8.6" excludeAll (
        ExclusionRule(organization = "org.ow2.asm")
      )
    )
  )

lazy val fetcher = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.8.0",
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      "com.jayway.jsonpath" % "json-path" % "2.6.0" excludeAll (
        ExclusionRule(organization = "org.ow2.asm")
      )
    )
  )

def cleanSparkMeta: Unit = {
  Folder.clean(file(".") / "spark" / "spark-warehouse",
               file(".") / "spark-warehouse",
               file(".") / "spark" / "metastore_db",
               file(".") / "metastore_db")
}

lazy val spark = project
  .dependsOn(aggregator.%("compile->compile;test->test"), fetcher)
  .settings(
    assembly / test := {},
    mainClass in (Compile, run) := Some(
      "ai.zipline.spark.Join"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
      "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided",
      "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
      "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
      "org.rogach" %% "scallop" % "4.0.1"
    ),
    testOptions in Test += Tests.Setup(() => cleanSparkMeta),
    testOptions in Test += Tests.Cleanup(() => cleanSparkMeta)
  )

exportJars := true

// TODO add benchmarks - follow this example
// https://github.com/sksamuel/avro4s/commit/781aa424f4affc2b8dfa35280c583442960df08b
//assemblyMergeStrategy in assembly := {
//  case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
//  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
//  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
//  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
//  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
//  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
//  case "about.html"                                 => MergeStrategy.rename
//  case "META-INF/ECLIPSEF.RSA"                      => MergeStrategy.last
//  case "META-INF/mailcap"                           => MergeStrategy.last
//  case "META-INF/mimetypes.default"                 => MergeStrategy.last
//  case "plugin.properties"                          => MergeStrategy.last
//  case "log4j.properties"                           => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}

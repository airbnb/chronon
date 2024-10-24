import sbt.Keys._
import sbt.Test

import scala.io.StdIn
import scala.sys.process._
import complete.DefaultParsers._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.6"
lazy val spark2_4_0 = "2.4.0"
lazy val spark3_1_1 = "3.1.1"
lazy val spark3_2_1 = "3.2.1"
lazy val tmp_warehouse = "/tmp/chronon/"

ThisBuild / organization := "ai.chronon"
ThisBuild / organizationName := "chronon"
ThisBuild / scalaVersion := scala212
ThisBuild / description := "Chronon is a feature engineering platform"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/airbnb/chronon"),
    "scm:git@github.com:airbnb/chronon.git"
  )
)
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / homepage := Some(url("https://github.com/airbnb/chronon"))
// The id needs to be sonatype id. TODO: ask the team to create one and add here.
ThisBuild / developers := List(
  Developer(
    id = "nikhilsimha",
    name = "Nikhil Simha",
    email = "r.nikhilsimha@gmail.com",
    url = url("http://nikhilsimha.com")
  )
)
ThisBuild / assembly / test := {}

def buildTimestampSuffix = ";build.timestamp=" + new java.util.Date().getTime
lazy val publishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) {
      Some("snapshots" at sys.env.getOrElse("CHRONON_SNAPSHOT_REPO", "unknown-repo") + buildTimestampSuffix)
    } else {
      val nexus = "https://s01.oss.sonatype.org/"
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  },
  publishMavenStyle := true
)

// Release related configs
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
lazy val releaseSettings = Seq(
  releaseUseGlobalVersion := false,
  releaseVersionBump := sbtrelease.Version.Bump.Next,
  // This step has internal issues working for downstream builds (workaround in place).
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    //runTest,                                        // Skipping tests as part of release process
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    // publishArtifacts,                              // This native step doesn't handle gpg signing (workaround below)
    releaseStepCommandAndRemaining("+ publishSigned"),
    releaseStepInputTask(releasePromptTask), // Manual user prompt to wait for confirmation before proceeding
    releaseStepInputTask(python_api, " release"), // This step handles the release of Python packages
    setNextVersion,
    commitNextVersion
    //pushChanges                                     // : Pushes the local Git changes to GitHub
  )
)

enablePlugins(GitVersioning, GitBranchPrompt)

lazy val supportedVersions = List(scala211, scala212, scala213)

lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark_uber, spark_embedded)
  .settings(
    publish / skip := true,
    crossScalaVersions := Nil,
    name := "chronon",
    version := git.versionProperty.value
  )
  .settings(releaseSettings: _*)

// Git related config
git.useGitDescribe := true
git.gitTagToVersionNumber := { tag: String => {
    // Git plugin will automatically add SNAPSHOT for dirty workspaces so remove it to avoid duplication.
    val versionStr = if (git.gitUncommittedChanges.value) version.value.replace("-SNAPSHOT", "") else version.value
    val branchTag = git.gitCurrentBranch.value.replace("/", "-")
    if (branchTag == "main" || branchTag == "master") {
      // For main branches, we tag the packages as <package-name>-<build-version>
      Some(s"${versionStr}")
    } else {
      // For user branches, we tag the packages as <package-name>-<user-branch>-<build-version>
      Some(s"${branchTag}-${versionStr}")
    }
  }
}
git.versionProperty := {
  val versionStr = version.value
  val branchTag = git.gitCurrentBranch.value.replace("/", "-")
  if (branchTag == "main" || branchTag == "master") {
    // For main branches, we tag the packages as <package-name>-<build-version>
    s"${versionStr}"
  } else {
    // For user branches, we tag the packages as <package-name>-<user-branch>-<build-version>
    s"${branchTag}-${versionStr}"
  }
}

/**
  * Versions are look up from mvn central - so we are fitting for three configurations
  * scala 11 + spark 2.4: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11/2.4.0
  * scala 12 + spark 3.1.1: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.1.1
  * scala 13 + spark 3.2.1: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.13/3.2.1
  */
val VersionMatrix: Map[String, VersionDependency] = Map(
  "spark-sql" -> VersionDependency(
    Seq(
      "org.apache.spark" %% "spark-sql",
      "org.apache.spark" %% "spark-core"
    ),
    Some(spark2_4_0),
    Some(spark3_1_1),
    Some(spark3_2_1)
  ),
  "spark-all" -> VersionDependency(
    Seq(
      "org.apache.spark" %% "spark-sql",
      "org.apache.spark" %% "spark-hive",
      "org.apache.spark" %% "spark-core",
      "org.apache.spark" %% "spark-streaming",
      "org.apache.spark" %% "spark-sql-kafka-0-10"
    ),
    Some(spark2_4_0),
    Some(spark3_1_1),
    Some(spark3_2_1)
  ),
  "scala-reflect" -> VersionDependency(
    Seq("org.scala-lang" % "scala-reflect"),
    Some(scala211),
    Some(scala212),
    Some(scala213)
  ),
  "scala-parallel-collections" -> VersionDependency(
    Seq("org.scala-lang.modules" %% "scala-parallel-collections"),
    None,
    None,
    Some("1.0.4")
  ),
  "delta-core" -> VersionDependency(
    Seq(
      "io.delta" %% "delta-core"
    ),
    Some("0.6.1"),
    Some("1.0.1"),
    Some("2.0.2")
  ),
  "jackson" -> VersionDependency(
    Seq(
      "com.fasterxml.jackson.core" % "jackson-core",
      "com.fasterxml.jackson.core" % "jackson-databind",
      "com.fasterxml.jackson.module" %% "jackson-module-scala"
    ),
    Some("2.6.7"),
    Some("2.10.0"),
    Some("2.12.3")
  ),
  "avro" -> VersionDependency(
    Seq(
      "org.apache.avro" % "avro"
    ),
    Some("1.8.2"),
    Some("1.8.2"),
    Some("1.10.2")
  ),
  "flink" -> VersionDependency(
    Seq(
      "org.apache.flink" %% "flink-streaming-scala",
      "org.apache.flink" % "flink-metrics-dropwizard",
      "org.apache.flink" % "flink-clients",
      "org.apache.flink" % "flink-test-utils"
    ),
    None,
    Some("1.16.1"),
    None
  ),
  "netty-buffer" -> VersionDependency(
    Seq(
      "io.netty" % "netty-buffer"
    ),
    None,
    None,
    Some("4.1.68.Final")
  )
)

def fromMatrix(scalaVersion: String, modules: String*): Seq[ModuleID] =
  modules.flatMap { module =>
    var mod = module
    val provided = module.endsWith("/provided")
    if (provided) {
      mod = module.replace("/provided", "")
    }
    assert(VersionMatrix.contains(mod),
           s"Version matrix doesn't contain module: $mod, pick one of ${VersionMatrix.keys.toSeq}")
    val result = VersionMatrix(mod).of(scalaVersion)
    if (provided) result.map(_ % "provided") else result
  }

lazy val api = project
  .settings(
    publishSettings,
    version := git.versionProperty.value,
    Compile / sourceGenerators += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    sourceGenerators in Compile += python_api_build.taskValue,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++=
      fromMatrix(scalaVersion.value, "spark-sql/provided") ++
        Seq(
          "org.apache.thrift" % "libthrift" % "0.13.0",
          "org.scala-lang" % "scala-reflect" % scalaVersion.value,
          "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0",
          "com.novocode" % "junit-interface" % "0.11" % "test",
          "org.scalatest" %% "scalatest" % "3.2.15" % "test",
          "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % "test"
        ),
  )

lazy val py_thrift = taskKey[Seq[File]]("Build thrift generated files")
py_thrift := {
  val apiDirectory = baseDirectory.value / "api"
  val inputThrift = apiDirectory / "thrift" / "api.thrift"
  val outputPy = apiDirectory / "py" / "ai" / "chronon"
  Thrift.gen(inputThrift.getPath, outputPy.getPath, "py", "api")
}

lazy val python_api_build = taskKey[Seq[File]]("Build thrift generated files")
ThisBuild / python_api_build := {
  python_api.toTask(" build").value
  Seq()
}

// Task for building Python API of Chronon
lazy val python_api = inputKey[Unit]("Build Python API")
python_api := {
  // Sbt has limited support for python and thus we are using bash script to achieve the same.
  val action = spaceDelimited("<arg>").parsed.headOption.getOrElse("build")
  val thrift = py_thrift.value
  val s: TaskStreams = streams.value
  val versionStr = (api / version).value
  val branchStr = git.gitCurrentBranch.value.replace("/", "-")
  s.log.info(s"Building Python API version: ${versionStr}, branch: ${branchStr}, action: ${action} ...")
  if ((s"api/py/python-api-build.sh ${versionStr} ${branchStr} ${action}" !) == 0) {
    s.log.success("Built Python API")
  } else {
    throw new IllegalStateException("Python API build failed!")
  }
}

lazy val releasePromptTask = inputKey[Unit]("Prompt the user for a yes/no answer")
releasePromptTask := {
  var wait = true
  while (wait) {
    println(s"""
            |[WARNING] Scala artifacts have been published to the Sonatype staging.
            |Please verify the Java builds are in order before proceeding with the Python API release.
            |Python release is irreversible. So proceed with caution.
            |""".stripMargin)
    val userInput = StdIn.readLine(s"Do you want to continue with the release: (y)es (n)o ? ").trim.toLowerCase
    if (userInput == "yes" || userInput == "y") {
      println("Continuing with the Python API release..")
      wait = false
    } else if (userInput == "no" || userInput == "n") {
      println("User selected `no`. Exiting the release process. Clear the workspace and release again as required.")
      sys.exit(0)
    } else {
      println(s"Invalid input: $userInput")
      // Keep prompting the user for valid input
    }
  }
}

lazy val aggregator = project
  .dependsOn(api.%("compile->compile;test->test"))
  .settings(
    publishSettings,
    version := git.versionProperty.value,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++=
      fromMatrix(scalaVersion.value, "spark-sql/provided") ++ Seq(
        "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
        "com.google.code.gson" % "gson" % "2.8.6"
      )
  )

lazy val online = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    publishSettings,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      // statsd 3.0 has local aggregation - TODO: upgrade
      "com.datadoghq" % "java-dogstatsd-client" % "2.7",
      "org.rogach" %% "scallop" % "4.0.1",
      "net.jodah" % "typetools" % "0.4.1",
      "com.github.ben-manes.caffeine" % "caffeine" % "2.8.5"
    ),
    libraryDependencies ++= fromMatrix(scalaVersion.value, "spark-all", "scala-parallel-collections", "netty-buffer"),
    version := git.versionProperty.value
  )

lazy val online_unshaded = (project in file("online"))
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    target := target.value.toPath.resolveSibling("target-no-assembly").toFile,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      // statsd 3.0 has local aggregation - TODO: upgrade
      "com.datadoghq" % "java-dogstatsd-client" % "2.7",
      "org.rogach" %% "scallop" % "4.0.1",
      "net.jodah" % "typetools" % "0.4.1",
      "com.github.ben-manes.caffeine" % "caffeine" % "2.8.5"
    ),
    libraryDependencies ++= fromMatrix(scalaVersion.value,
                                       "jackson",
                                       "avro",
                                       "spark-all/provided",
                                       "scala-parallel-collections",
                                       "netty-buffer")
  )


def cleanSparkMeta(): Unit = {
  Folder.clean(file(".") / "spark" / "spark-warehouse*",
               file(tmp_warehouse) / "spark-warehouse*",
               file(".") / "spark" / "metastore_db*",
               file(tmp_warehouse) / "metastore_db*")
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
  libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4" % Test
) ++ addArtifact(assembly / artifact, assembly) ++ publishSettings

lazy val spark_uber = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online_unshaded)
  .settings(
    sparkBaseSettings,
    version := git.versionProperty.value,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= fromMatrix(scalaVersion.value, "jackson", "spark-all/provided", "delta-core/provided")
  )

lazy val spark_embedded = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online_unshaded)
  .settings(
    sparkBaseSettings,
    version := git.versionProperty.value,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= fromMatrix(scalaVersion.value, "spark-all", "delta-core"),
    target := target.value.toPath.resolveSibling("target-embedded").toFile,
    Test / test := {}
  )

lazy val flink = (project in file("flink"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    publishSettings,
    crossScalaVersions := List(scala212),
    libraryDependencies ++= fromMatrix(scalaVersion.value,
                                       "avro",
                                       "spark-all/provided",
                                       "scala-parallel-collections",
                                       "flink")
  )

// Build Sphinx documentation
lazy val sphinx = taskKey[Unit]("Build Sphinx Documentation")
sphinx := {
  // Sbt has limited support for Sphinx and thus we are using bash script to achieve the same.
  // To generate Python API docs, we need to finish the build of Python API.
  python_api_build.value
  val s: TaskStreams = streams.value
  s.log.info("Building Sphinx documentation...")
  if (("docs/build-sphinx.sh" !) == 0) {
    s.log.success("Built Sphinx documentation")
  } else {
    throw new IllegalStateException("Sphinx build failed!")
  }
}

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _ @_*)         => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case PathList("com", "fasterxml", _ @_*) => MergeStrategy.last
  case PathList("com", "google", _ @_*)    => MergeStrategy.last
  case _                                   => MergeStrategy.first
}
exportJars := true




import sbt.Keys._
import sbt.Test
import scala.sys.process._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.6"


ThisBuild / organization := "ai.chronon"
ThisBuild / organizationName := "chronon"
ThisBuild / scalaVersion := scala211
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

lazy val publishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) {
      Some("snapshots" at sys.env.get("CHRONON_SNAPSHOT_REPO").getOrElse("unknown-repo") + "/")
    } else {
      val nexus = "https://s01.oss.sonatype.org/"
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  },
  publishMavenStyle := true
)

// Release related configs
import ReleaseTransformations._
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
    setNextVersion,
    commitNextVersion,
    //pushChanges                                     // : Pushes the local Git changes to GitHub
  )
)

lazy val supportedVersions = List(scala211, scala212, scala213)

lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark_uber, spark_embedded)
  .settings(
    publish / skip := true,
    crossScalaVersions := Nil,
    name := "chronon"
  )
  .settings(releaseSettings: _*)
  .enablePlugins(GitVersioning, GitBranchPrompt)

// Git related config
git.useGitDescribe := true
git.gitTagToVersionNumber := { tag: String =>
  // Git plugin will automatically add SNAPSHOT for dirty workspaces so remove it to avoid duplication.
  val versionStr = if (git.gitUncommittedChanges.value) version.value.replace("-SNAPSHOT", "") else version.value
  val branchTag = git.gitCurrentBranch.value
  // [stripe] We tag all packages consistently irrespective of the branch being used and
  // rely on appropriate versioning
  // For master branches, we tag the packages as <package-name>-<build-version>
  Some(s"${versionStr}")
}

lazy val api = project
  .settings(
    publishSettings,
    // TODO(andrewlee) the Thrift.gen() call below autogenerates Thrift java classes as part of the sbt build.
    //  It works in CI but appears to silently fail when running locally. Right now we can get around that
    //  by manually running thrift from the cli (see README) but I plan to look into this more later.
    sourceGenerators in Compile += Def.task {
      val inputThrift = baseDirectory.value / "thrift" / "api.thrift"
      val outputJava = (Compile / sourceManaged).value
      Thrift.gen(inputThrift.getPath, outputJava.getPath, "java")
    }.taskValue,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.13.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.10",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.10",
      "org.scala-lang" % "scala-reflect" % "2.11.12",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.9.10",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0",
      "org.slf4j" % "slf4j-api" % "1.7.30",
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",
      "com.novocode" % "junit-interface" % "0.11" % "test"
    ),
  )

lazy val aggregator = project
  .dependsOn(api.%("compile->compile;test->test"))
  .settings(
    publishSettings,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
      "com.google.code.gson" % "gson" % "2.8.6"
    )
  )

lazy val online = project
  .dependsOn(aggregator.%("compile->compile;test->test"))
  .settings(
    publishSettings,
    crossScalaVersions := supportedVersions,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
      // [TODO: stripe] Upstream this change as we're on 4.1.0 internally
      "com.datadoghq" % "java-dogstatsd-client" % "4.1.0",
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

def sparkLibs(version: String): Seq[sbt.librarymanagement.ModuleID] = Seq(
  "org.apache.spark" %% "spark-sql" % version,
  "org.apache.spark" %% "spark-hive" % version,
  "org.apache.spark" %% "spark-core" % version,
  "org.apache.spark" %% "spark-streaming" % version,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % version
)

val sparkBaseSettings: Seq[Setting[_]] = Seq(
  assembly / test := {},
  assembly / artifact := {
    val art = (assembly / artifact).value
    art.withClassifier(Some("assembly"))
  },
  mainClass in (Compile, run) := Some("ai.chronon.spark.Driver"),
  cleanFiles ++= Seq(
    baseDirectory.value / "spark-warehouse",
    baseDirectory.value / "metastore_db"
  ),
  testOptions in Test += Tests.Setup(() => cleanSparkMeta()),
  testOptions in Test += Tests.Cleanup(() => cleanSparkMeta()),
  // compatibility for m1 chip laptop
  libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4" % Test
) ++ addArtifact(assembly / artifact, assembly) ++ publishSettings

// TODO: use github releases to publish the spark driver
val embeddedAssemblyStrategy: Setting[_] = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _ @_*)         => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case _                                   => MergeStrategy.first
}

// Hit errors on the lines of:
// deduplicate: different file contents found in the following:
// .. gson-2.8.6.jar:module-info.class
val uberAssemblyStrategy: Setting[_] = assemblyMergeStrategy in assembly := {
  case x if x.endsWith("module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val spark_uber = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    sparkBaseSettings,
    uberAssemblyStrategy,
    crossScalaVersions := Seq(scala211, scala212),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, major)) if major == 12 =>
          sparkLibs("3.1.1").map(_ % "provided")
        case _ =>
          sparkLibs("2.4.0").map(_ % "provided")
      }
    }
  )

// Project for running with embedded spark for local testing
lazy val spark_embedded = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    sparkBaseSettings,
    libraryDependencies ++= sparkLibs("2.4.0"),
    target := target.value.toPath.resolveSibling("target-embedded").toFile,
    embeddedAssemblyStrategy,
    Test / test := {}
  )

// Build Sphinx documentation
lazy val sphinx = taskKey[Unit]("Build Sphinx Documentation")
sphinx := {
  // Sbt has limited support for Sphinx and thus we are using bash script to achieve the same.
  val s: TaskStreams = streams.value
  s.log.info("Building Sphinx documentation...")
  if (("docs/build-sphinx.sh" !) == 0) {
    s.log.success("Built Sphinx documentation")
  } else {
    throw new IllegalStateException("Sphinx build failed!")
  }
}

exportJars := true

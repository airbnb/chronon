import sbt.Keys._
import sbt.Test

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.6"

// Release related configs
import ReleaseTransformations._
lazy val releaseSettings = Seq(
  releaseUseGlobalVersion := false,
  releaseVersionBump := sbtrelease.Version.Bump.Minor,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,              // : ReleaseStep
    inquireVersions,                        // : ReleaseStep
    runClean,                               // : ReleaseStep
    //runTest,                                // : ReleaseStep
    setReleaseVersion,                      // : ReleaseStep
    commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
    tagRelease,                             // : ReleaseStep
    releaseStepCommandAndRemaining("+ publishSigned"),  // : ReleaseStep, checks whether `publishTo` is properly set up
    setNextVersion,                         // : ReleaseStep
    commitNextVersion                      // : ReleaseStep
    //pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
  )
)

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
    val nexus = "https://s01.oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at "https://artifactory.d.musta.ch/artifactory/maven-airbnb-snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true
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
  if (branchTag == "master") {
    // For master branches, we tag the packages as <package-name>-<build-version>
    Some(s"${versionStr}")
  } else {
    // For user branches, we tag the packages as <package-name>-<user-branch>-<build-version>
    Some(s"${branchTag}-${versionStr}")
  }
}

lazy val api = project
  .settings(
    publishSettings,
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
      // statsd 3.0 has local aggregation - TODO: upgrade
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

lazy val spark_uber = (project in file("spark"))
  .dependsOn(aggregator.%("compile->compile;test->test"), online)
  .settings(
    sparkBaseSettings,
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

exportJars := true

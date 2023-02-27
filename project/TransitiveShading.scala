import com.eed3si9n.jarjarabrams.ShadeRule
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._

object TransitiveShading {
  def project(name: String,
              dependencies: Seq[ModuleID],
              shadeRules: Seq[ShadeRule],
              crossVersions: List[String]): Unit = {
    Project(
      id = s"chronon-shaded-$name",
      base = file(s"shaded/$name")
    ).settings(
      assembly / test := {},
      assemblyPackageScala / assembleArtifact := false,
      assembly / assemblyJarName := {
        s"${name}_${scalaBinaryVersion.value}-${version.value}.jar"
      },
      crossScalaVersions := crossVersions,
      assembly / assemblyShadeRules := shadeRules,
      assembly / target := baseDirectory.value.getParentFile / "target" / scalaBinaryVersion.value,
      addArtifact(Artifact(s"chronon-shaded-$name"), assembly),
      libraryDependencies ++= dependencies
    )
  }

  def changeShadedDeps(toExclude: Set[String], toInclude: List[xml.Node], node: xml.Node): xml.Node = {
    node match {
      case elem: xml.Elem =>
        val child =
          if (elem.label == "dependencies") {
            elem.child.filterNot { dep =>
              dep.child.find(_.label == "groupId").exists(gid => toExclude.contains(gid.text))
            } ++ toInclude
          } else {
            elem.child.map(changeShadedDeps(toExclude, toInclude, _))
          }
        xml.Elem(elem.prefix, elem.label, elem.attributes, elem.scope, false, child: _*)
      case _ =>
        node
    }
  }

  def getShadedDepXML(groupId: String, artifactId: String, version: String, scope: String): scala.xml.Node = {
    <dependency>
      <groupId>{groupId}</groupId>
      <artifactId>{artifactId}</artifactId>
      <version>{version}</version>
      <scope>{scope}</scope>
    </dependency>
  }
}

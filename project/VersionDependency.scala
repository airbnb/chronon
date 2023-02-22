import sbt.librarymanagement.{CrossVersion, ModuleID}
import sbt.librarymanagement.DependencyBuilders.OrganizationArtifactName

case class VersionDependency(modules: Seq[OrganizationArtifactName],
                             v11: Option[String],
                             v12: Option[String],
                             v13: Option[String]) {
  def of(scalaVersion: String): Seq[ModuleID] = {
    def applyVersion(v: Option[String]): Seq[ModuleID] = v.map(ver => modules.map(_.%(ver))).getOrElse(Seq.empty)
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 11)) => applyVersion(v11)
      case Some((2, 12)) => applyVersion(v12)
      case Some((2, 13)) => applyVersion(v13)
      case _ =>
        throw new RuntimeException(s"Unhandled scala version $scalaVersion for modules ${modules.map(_.toString)}")
    }
  }
}

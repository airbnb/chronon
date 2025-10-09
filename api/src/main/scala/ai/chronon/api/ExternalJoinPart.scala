package ai.chronon.api

class ExternalJoinPart(joinPart: JoinPart, fullPrefix: String) extends JoinPart(joinPart) {
  lazy val externalJoinFullPrefix: String = fullPrefix

  override def deepCopy(): JoinPart = {
    new ExternalJoinPart(joinPart.deepCopy(), externalJoinFullPrefix)
  }
}

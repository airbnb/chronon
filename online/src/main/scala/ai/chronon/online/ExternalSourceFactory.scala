package ai.chronon.online

import ai.chronon.api.ExternalSource

abstract class ExternalSourceFactory extends Serializable {
  def createExternalSourceHandler(externalSource: ExternalSource): ExternalSourceHandler
}
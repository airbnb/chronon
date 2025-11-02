package ai.chronon.online

import ai.chronon.api.ExternalSource

/**
  * Factory interface for creating external source handlers dynamically.
  *
  * Implementations of this factory can be registered with ExternalSourceRegistry
  * to enable dynamic creation of ExternalSourceHandler instances based on
  * external source configurations at runtime.
  *
  * The factory pattern allows for flexible handler creation where the specific
  * handler implementation can be determined by the ExternalSource configuration,
  * including factory parameters passed through ExternalSourceFactoryConfig.
  */
abstract class ExternalSourceFactory extends Serializable {

  /**
    * Creates an ExternalSourceHandler instance for the given external source.
    *
    * @param externalSource The external source configuration containing metadata,
    *                      schema information, and factory configuration with parameters
    * @return A new ExternalSourceHandler instance configured for the external source
    */
  def createExternalSourceHandler(externalSource: ExternalSource): ExternalSourceHandler
}

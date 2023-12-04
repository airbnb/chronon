package ai.chronon.online

object Config {
  // Check value of environment variable or system property
  def getEnvConfig(configName: String): Option[String] =
    Option(System.getProperty(configName))
      .orElse(Option(System.getenv(configName)))

  // Check value of environment variable or system property. Return default if not
  def getEnvConfig(configName: String, default: String): String =
    Option(System.getProperty(configName))
      .orElse(Option(System.getenv(configName)))
      .getOrElse(default)

  // Check value of environment variable or system property. Return default if not
  def getEnvConfig(configName: String, default: Boolean): Boolean =
    Option(System.getProperty(configName))
      .orElse(Option(System.getenv(configName)))
      .getOrElse(default.toString)
      .toBoolean

  // Check value of environment variable or system property. Return default if not
  def getEnvConfig(configName: String, default: Int): Int =
    Option(System.getProperty(configName))
      .orElse(Option(System.getenv(configName)))
      .getOrElse(default.toString)
      .toInt

}

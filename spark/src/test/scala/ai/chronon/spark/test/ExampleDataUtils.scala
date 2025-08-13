package ai.chronon.spark.test

import java.io.File

object ExampleDataUtils {
  def getExampleDataDirectory(): String = {
    val confResource = getClass.getResource("/")
    if (confResource != null) {
      confResource.getPath
    } else {
      // Fallback to a relative path for test resources
      "spark/src/test/resources"
    }
  }

  def getExampleData(path: String): String =
    new File(getExampleDataDirectory(), path).getPath
}

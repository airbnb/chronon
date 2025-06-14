package ai.chronon.spark.test

import com.google.devtools.build.runfiles.Runfiles
import java.io.File

object ExampleDataUtils {
  lazy val runfiles = Runfiles.create()

  def getExampleDataDirectory(): String = {
    val confResource = getClass.getResource("/")
    if (confResource != null) confResource.getPath
    else runfiles.rlocation("chronon/spark/src/test/resources")
  }

  def getExampleData(path: String): String =
    new File(getExampleDataDirectory(), path).getPath
}

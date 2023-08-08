package ai.chronon.api

import java.io.File

object ConfUtils {

  def validateChrononWorkingDirectory: Unit = {
    val cwd = System.getProperty("user.dir")
    val validDirectoryName = cwd.endsWith("chronon") || cwd.endsWith("zipline")
    assert(validDirectoryName, "cwd is not a valid Chronon directory - root folder is not named chronon or zipline")
    val productionDir = new File(s"$cwd/production");
    val containsProductionDir = productionDir.exists() && productionDir.isDirectory
    assert(containsProductionDir, "cwd is not a valid Chronon directory - does not contain a 'production' folder")
    val teamsJsonFile = new File(s"$cwd/teams.json");
    val teamsJsonExists = teamsJsonFile.exists() && !teamsJsonFile.isDirectory
    assert(teamsJsonExists, "cwd is not a valid Chronon directory - does not contain a 'teams.json' file")
  }
}

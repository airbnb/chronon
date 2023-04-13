package ai.chronon.api

import scala.collection.mutable

// takes a map of macro names and functions and applies the functions on macro arguments
case class ParametricMacro(value: String, func: Map[String, String] => String) {
  private val pattern = s"""\\{\\{\\s*$value((:[0-9A-Za-z_]*=[0-9A-Za-z_,.]*)*)\\s*}}""".r

  def replace(str: String): String = {
    var startIndex = 0
    val fragments = new mutable.ArrayBuffer[String] {}
    pattern.findAllMatchIn(str) foreach {
      m =>
        fragments.append(str.substring(startIndex, m.start))
        val args = m.group(1)
        val argMap = args.split(":").filter(_.nonEmpty).map(_.split("=")).map(x => x(0) -> x(1)).toMap
        val result = func(argMap)
        fragments.append(result)
        startIndex = m.end
    }
    fragments.append(str.substring(startIndex, str.length))
    fragments.mkString("")
  }
}
package ai.chronon.api

import scala.collection.mutable

// takes a map of macro names and functions and applies the functions on macro arguments
case class ParametricMacro(value: String, func: Map[String, String] => String) {
  private val pattern = s"""\\{\\{\\s*$value(\\([\\s0-9A-Za-z_.,=]*\\))*\\s*}}""".r

  def replace(str: String): String = {
    var startIndex = 0
    val fragments = new mutable.ArrayBuffer[String] {}
    pattern.findAllMatchIn(str) foreach {
      m =>
        fragments.append(str.substring(startIndex, m.start))
        val argMap = Option(m.group(1)).map { args =>
          val inner = args.substring(1, args.length - 1)
          val parsed = inner.split(",").foldLeft(Seq.empty[String]) { case (argSeq, token) =>
            assert(token.count(_ == '=') <= 1)
            if (token.contains("=")) {
              argSeq :+ token
            } else {
              argSeq.tail :+ (argSeq.head + "," + token)
            }
          }
          println(parsed)
          parsed.map(_.split("=").map(_.trim)).map(x => x(0) -> x(1)).toMap
        }
        val result = func(argMap.getOrElse(Map.empty[String, String]))
        fragments.append(result)
        startIndex = m.end
    }
    fragments.append(str.substring(startIndex, str.length))
    fragments.mkString("")
  }
}

object ParametricMacro {
  def main(args: Array[String]): Unit = {
    val mc = ParametricMacro("something", { x => "st:" + x.keys.mkString("/") + "|" + x.values.mkString("/") })
    val str = "something nothing-{{ something( a_1=b,, 3.1, c=d) }}-something after-{{ thing:a1=b1 }}{{ something }}"
    val replaced = mc.replace(str)
    println(replaced)
  }
}
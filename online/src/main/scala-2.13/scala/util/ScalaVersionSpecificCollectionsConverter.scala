package scala.util

import scala.jdk.CollectionConverters._

object ScalaVersionSpecificCollectionsConverter {

  def convertScalaMapToJava[S,T](map: Map[S,T]) : java.util.Map[S,T] = {
    map.asJava
  }

  def convertJavaMapToScala[S,T](map: java.util.Map[S,T]) : Map[S,T] = {
    map.asScala.toMap
  }
}

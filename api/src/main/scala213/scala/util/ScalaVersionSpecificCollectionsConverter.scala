package scala.util

import scala.jdk.CollectionConverters._

object ScalaVersionSpecificCollectionsConverter {

  def convertScalaMapToJava[S,T](map: Map[S,T]) : java.util.Map[S,T] = {
    map.asJava
  }

  def convertJavaMapToScala[S,T](map: java.util.Map[S,T]) : Map[S,T] = {
    map.asScala.toMap
  }

  def convertScalaListToJava[S](map: List[S]) : java.util.List[S] = {
    map.asJava
  }

  def convertJavaListToScala[S](map: java.util.List[S]) : List[S] = {
    map.asScala.toList
  }
}

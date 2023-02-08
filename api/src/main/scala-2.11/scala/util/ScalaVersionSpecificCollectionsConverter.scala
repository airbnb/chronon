package scala.util

import scala.collection.JavaConverters._

object ScalaVersionSpecificCollectionsConverter {

  def convertScalaMapToJava[S, T](map: Map[S, T]): java.util.Map[S, T] = {
    map.asJava
  }

  def convertJavaMapToScala[S, T](map: java.util.Map[S, T]): Map[S, T] = {
    map.asScala.toMap
  }

  def convertScalaListToJava[S](map: List[S]): java.util.List[S] = {
    map.asJava
  }

  def convertScalaSeqToJava[S](seq: Seq[S]): java.util.List[S] = {
    seq.asJava
  }

  def convertJavaListToScala[S](map: java.util.List[S]): List[S] = {
    map.asScala.toList
  }
}

object ScalaToJavaConversions {
  implicit class IteratorOps[T](iterator: java.util.Iterator[T]) {
    def toScala: Iterator[T] = {
      iterator.asScala
    }
  }
}

package scala.util

import java.util
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.parallel.ParSeq
import scala.jdk.CollectionConverters._

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

  def convertJavaListToScala[S](jList: java.util.List[S]): List[S] = {
    jList.asScala.toList
  }
}

object ScalaJavaConversions {

  implicit class IteratorOps[T](iterator: java.util.Iterator[T]) {
    def toScala: Iterator[T] = {
      iterator.asScala
    }
  }
  implicit class JIteratorOps[T](iterator: Iterator[T]) {
    def toJava: java.util.Iterator[T] = {
      iterator.asJava
    }
  }
  implicit class ListOps[T](list: java.util.List[T]) {
    def toScala: List[T] = {
      if (list == null) {
        null
      } else {
        list.iterator().asScala.toList
      }
    }
  }
  implicit class JListOps[T](list: Seq[T]) {
    def toJava: java.util.List[T] = {
      if (list == null) {
        null
      } else {
        list.asJava
      }
    }
  }
  implicit class IterableOps[T](it: Iterable[T]) {
    def parallel: ParSeq[T] = {
      if (it == null) {
        null
      } else {
        it.toSeq.par
      }
    }
  }
  implicit class MapOps[K, V](map: java.util.Map[K, V]) {
    def toScala: Map[K, V] = {
      if (map == null) {
        null
      } else {
        map.asScala.toMap
      }
    }
  }
  implicit class JMapOps[K, V](map: Map[K, V]) {
    def toJava: java.util.Map[K, V] = {
      if (map == null) {
        null
      } else {
        map.asJava
      }
    }
  }
}

/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package scala.util

import scala.collection.parallel.CollectionConverters.IterableIsParallelizable
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

  def convertJavaListToScala[S](map: java.util.List[S]): List[S] = {
    map.asScala.toList
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
  implicit class JListSeqOps[T](list: scala.collection.Seq[T]) {
    def toJava: java.util.List[T] = {
      if (list == null) {
        null
      } else {
        list.asJava
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
  implicit class IterableOps[T](it: Iterable[T]) {
    def parallel: ParSeq[T] = {
      if (it == null) {
        null
      } else {
        it.toSeq.par.toSeq
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

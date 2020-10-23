package ai.zipline.aggregator.base

import java.util

import scala.reflect.ClassTag

// we are forced to use arrayList for sort-ability
object ArrayUtils {
  def fromArray[I](array: Array[I]): util.ArrayList[I] = {
    val result = new util.ArrayList[I](array.length)
    for (i <- array.indices) {
      result.add(array(i))
    }
    result
  }

  def toArray[I: ClassTag](list: util.ArrayList[I]): Array[I] = {
    val result = new Array[I](list.size)
    for (i <- 0 until list.size) {
      result.update(i, list.get(i))
    }
    result
  }
}

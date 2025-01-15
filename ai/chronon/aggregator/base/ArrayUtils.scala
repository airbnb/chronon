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

package ai.chronon.aggregator.base

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

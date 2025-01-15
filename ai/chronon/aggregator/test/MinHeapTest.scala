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

package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.MinHeap
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.collection.JavaConverters._

class MinHeapTest extends TestCase {
  def testInserts(): Unit = {
    val mh = new MinHeap[Int](maxSize = 4, Ordering.Int)

    def make_container = new util.ArrayList[Int](4)

    val arr1 = make_container
    mh.insert(arr1, 5)
    mh.insert(arr1, 4)
    mh.insert(arr1, 9)
    mh.insert(arr1, 10)
    mh.insert(arr1, -1)
    assertArrayEquals(arr1.asScala.toArray, Array(9, 4, 5, -1))

    val arr2 = make_container
    mh.insert(arr2, 5)
    mh.insert(arr2, 4)
    mh.insert(arr2, 9)
    mh.insert(arr2, 10)
    mh.insert(arr2, 1)
    mh.insert(arr2, 2)
    assertArrayEquals(arr2.asScala.toArray, Array(5, 4, 2, 1))

    mh.merge(arr1, arr2)
    assertArrayEquals(arr1.asScala.toArray, Array(4, 2, 1, -1))
  }
}

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

// An implementation of a fast non-strict min-heap,
// The topK isn't sorted, just collected until the size exceeds k
class MinHeap[T](maxSize: Int, ordering: Ordering[T]) {
  private val reverseComparator = ordering.reverse

  type ContainerType[T] = util.ArrayList[T]

  // take the first element of the array and swap it down until
  // it follows the heap conditions:
  // 1.) elem[i] > elem[2*i + 1]
  // 2.) elem[i] > elem[2*i + 2]
  private final def siftDownRoot(arr: ContainerType[T]): Unit = {
    var par = 0
    var left = (par << 1) + 1
    while (par >= 0 && left < arr.size) {
      var candidate = left
      var candidateVal = arr.get(left)
      val right = left + 1
      if (right < arr.size) {
        val rightVal = arr.get(right)
        if (ordering.lt(candidateVal, rightVal)) {
          candidate = right
          candidateVal = rightVal
        }
      }
      val parentVal = arr.get(par)
      if (ordering.gteq(parentVal, candidateVal)) {
        return
      }
      arr.set(par, candidateVal)
      arr.set(candidate, parentVal)
      par = candidate
      left = (par << 1) + 1
    }
  }

  // mutating arr
  def insert(arr: ContainerType[T], elem: T): ContainerType[T] = {
    if (arr.size < maxSize - 1) {
      arr.add(elem)
    } else if (arr.size == maxSize - 1) {
      // reverse sort when we hit the size limit
      // reverse sorting is equivalent to heap-ification
      arr.add(elem)
      arr.sort(reverseComparator)
    } else if (arr.size == maxSize) {
      // array is full, and the element is smaller than the largest,
      // add to the root and sift down
      if (ordering.lteq(elem, arr.get(0))) {
        arr.set(0, elem)
        siftDownRoot(arr)
      }
    }
    arr
  }

  //mutating arr1 / arr2 intact
  def merge(
      arr1: ContainerType[T],
      arr2: ContainerType[T]
  ): ContainerType[T] = {
    if (arr1.size() + arr2.size() < maxSize) {
      arr1.addAll(arr2)
    } else {
      val it = arr2.iterator
      while (it.hasNext) {
        insert(arr1, it.next())
      }
    }
    arr1
  }

  def sort(arr: ContainerType[T]): ContainerType[T] = {
    arr.sort(ordering)
    arr
  }
}

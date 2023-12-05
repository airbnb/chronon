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

import ai.chronon.aggregator.base.ApproxDistinctCount
import junit.framework.TestCase
import org.junit.Assert._

class ApproxDistinctTest extends TestCase {
  def testErrorBound(uniques: Int, errorBound: Int, lgK: Int): Unit = {
    val uniqueElems = 1 to uniques
    val duplicates = uniqueElems ++ uniqueElems ++ uniqueElems
    val counter = new ApproxDistinctCount[Long](lgK)
    val ir = counter.prepare(duplicates.head)
    duplicates.tail.foreach { elem => counter.update(ir, elem) }
    val estimated = counter.finalize(ir)
    // println(s"estimated - $estimated, actual - $uniques, bound - $errorBound")
    assertTrue(Math.abs(estimated - uniques) < errorBound)
  }

  def testMergingErrorBound(uniques: Int, errorBound: Int, lgK: Int, merges: Int): Unit = {
    val chunkSize = uniques / merges
    assert(chunkSize > 0)
    val counter = new ApproxDistinctCount[Long](lgK)
    // slice the range 1 to uniques into n splits, where n = `merges`
    val uniqueElemsList = (0 until merges).map(merge => ((chunkSize * merge) + 1) to chunkSize * (merge + 1))
    val irList = uniqueElemsList.map { uniqueElems =>
      val duplicates = uniqueElems ++ uniqueElems ++ uniqueElems
      val ir = counter.prepare(duplicates.head)
      duplicates.tail.foreach { elem => counter.update(ir, elem) }
      ir
    }
    val ir = irList.reduceLeft(counter.merge)
    val estimated = counter.finalize(ir)
    // println(s"estimated - $estimated, actual - $uniques, bound - $errorBound")
    assertTrue(Math.abs(estimated - uniques) < errorBound)
  }

  def testErrorBounds(): Unit = {
    testErrorBound(uniques = 100, errorBound = 1, lgK = 10)
    testErrorBound(uniques = 1000, errorBound = 20, lgK = 10)
    testErrorBound(uniques = 10000, errorBound = 300, lgK = 10)
  }

  def testMergingErrorBounds(): Unit = {
    testMergingErrorBound(uniques = 100, errorBound = 1, lgK = 10, merges = 10)
    testMergingErrorBound(uniques = 1000, errorBound = 20, lgK = 10, merges = 4)
    testMergingErrorBound(uniques = 10000, errorBound = 400, lgK = 10, merges = 100)
  }
}

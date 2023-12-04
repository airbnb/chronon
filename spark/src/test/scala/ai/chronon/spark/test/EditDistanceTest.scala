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

package ai.chronon.spark.test

import org.junit.Assert.assertEquals
import org.junit.Test
import ai.chronon.spark.stats.EditDistance

class EditDistanceTest {

  @Test
  def basic(): Unit = {
    def of(a: Any, b: Any) = EditDistance.between(a, b)
    def ofString(a: String, b: String) = EditDistance.betweenStrings(a, b)

    assertEquals(0, of(null, null).total)
    assertEquals(0, of(Seq.empty[String], null).total)
    assertEquals(0, of(null, Seq.empty[String]).total)
    assertEquals(0, of(Seq.empty[String], Seq.empty[String]).total)
    assertEquals(2, of(Seq("abc", "def"), null).total)
    assertEquals(2, of(Seq("abc", "def"), Seq.empty[String]).total)
    assertEquals(0, of(Seq("abc", "def"), Seq("abc", "def")).total)
    assertEquals(2, of(Seq(3, 1), Seq(4, 3, 1, 2)).delete)

    // 2 deletes from & 3 inserts into right - to make it like left
    assertEquals(2, of(Seq(1, 2, 3, 4), Seq(5, 6, 2)).delete)
    assertEquals(3, of(Seq(1, 2, 3, 4), Seq(5, 6, 2)).insert)
    assertEquals(6, ofString("abc", "def").total)
    assertEquals(4, ofString("abc", "dbf").total)
  }
}

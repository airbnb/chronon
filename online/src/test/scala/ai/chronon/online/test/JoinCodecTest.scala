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

package ai.chronon.online.test

import ai.chronon.online.OnlineDerivationUtil.reintroduceExceptions
import org.junit.Assert.assertEquals
import org.junit.Test

class JoinCodecTest {
  @Test
  def testReintroduceException(): Unit = {

    val preDerived = Map("group_by_2_exception" -> "ex", "group_by_1_exception" -> "ex", "group_by_4_exception" -> "ex")
    val derived = Map(
      "group_by_1_feature1" -> "val1",
      "group_by_2_feature1" -> "val1",
      "group_by_2_feature2" -> "val2",
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2"
    )

    val result = reintroduceExceptions(derived, preDerived)

    val expected = Map(
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2",
      "group_by_2_exception" -> "ex",
      "group_by_1_exception" -> "ex",
      "group_by_4_exception" -> "ex"
    )
    assertEquals(expected, result)
  }
}

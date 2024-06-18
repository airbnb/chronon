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

package ai.chronon.api.test

import ai.chronon.api.ParametricMacro
import org.junit.Assert.assertEquals
import org.junit.Test

class ParametricMacroTest {
  @Test
  def testSubstitution(): Unit = {
    val mc = ParametricMacro("something", { x => "st:" + x.keys.mkString("/") + "|" + x.values.mkString("/") })
    val str = "something nothing-{{ something( a_1=b, 3.1, c=d) }}-something after-{{ thing:a1=b1 }}{{ something }}"
    val replaced = mc.replace(str)
    val expected = "something nothing-st:a_1/c|b, 3.1/d-something after-{{ thing:a1=b1 }}st:|"
    assertEquals(expected, replaced)
    val invalidArg = "something nothing-{{ something(a_1=b,3+1,c=d) }}-something after-{{ thing:a1=b1 }}{{ something }}"
    val replacedInvalid = mc.replace(invalidArg)
    val expectedInvalidArg = "something nothing-{{ something(a_1=b,3+1,c=d) }}-something after-{{ thing:a1=b1 }}st:|"
    assertEquals(expectedInvalidArg, replacedInvalid)
  }
}

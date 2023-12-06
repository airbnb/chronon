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

package ai.chronon.online

import ai.chronon.api.Builders
import ai.chronon.online.Metrics.{Context, Environment}
import org.junit.Assert.assertEquals
import org.junit.Test

class TagsTest {
  // test that ttlCache of context is creates non duplicated entries

  // copied from the private NonBlockingStatsDClient.tagString
  def tagString(tags: Array[String], tagPrefix: String): String = {
    var sb: StringBuilder = null
    if (tagPrefix != null) {
      if ((tags == null) || (tags.length == 0)) return tagPrefix
      sb = new StringBuilder(tagPrefix)
      sb.append(",")
    } else {
      if ((tags == null) || (tags.length == 0)) return ""
      sb = new StringBuilder("|#")
    }
    for (n <- tags.length - 1 to 0 by -1) {
      sb.append(tags(n))
      if (n > 0) sb.append(",")
    }
    sb.toString
  }

  @Test
  def testCachedTagsAreComputedTags(): Unit = {
    val cache = new TTLCache[Metrics.Context, String](
      { ctx => ctx.toTags.mkString(",") },
      { ctx => ctx },
      ttlMillis = 5 * 24 * 60 * 60 * 1000 // 5 days
    )
    val context = Metrics.Context(
      Environment.JoinOffline,
      Builders.Join(
        metaData = Builders.MetaData("join1", team = "team1"),
        left = Builders.Source.events(
          query = null,
          table = "table1"
        ),
        joinParts = Builders.JoinPart(
          groupBy = Builders.GroupBy(
            metaData = Builders.MetaData("group_by1", team = "team2"),
            sources = Seq(
              Builders.Source.events(
                query = null,
                table = "table2"
              )
            )
          )
        ) :: Nil
      )
    )
    val copyFake = context.copy(join = "something else")
    val copyCorrect = copyFake.copy(join = context.join)

    // add three entires to cache - two distinct contexts and one copy of the first
    val original = cache(context)
    val copied = cache(copyCorrect)
    val copiedFake = cache(copyFake)
    assertEquals(cache.cMap.size(), 2)

    val slowTags = tagString(context.toTags, null)
    val fastTags = tagString(Array(Context.tagCache(copyCorrect)), null)
    assertEquals(slowTags, fastTags)
  }

}

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

import ai.chronon.api.QueryUtils
import ai.chronon.spark.SparkSessionBuilder
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

/**
 * Verifies the flat-SQL semantics used by JoinSourceRunner.applyQuery after the two-stage
 * selectExpr→filter approach was replaced with temp-view + session.sql(QueryUtils.build(...)).
 *
 * The key invariant: wheres resolve against the pre-select (raw) input schema, not against aliases
 * defined in selects. This matches offline backfill and non-JoinSource streaming behavior.
 */
class JoinSourceRunnerApplyQueryTest extends TestCase {

  val spark: SparkSession = SparkSessionBuilder.build("JoinSourceRunnerApplyQueryTest", local = true)

  import spark.implicits._

  private def makeView(viewName: String): Unit = {
    // DataFrame with raw column names that selects may rename/transform
    val df = Seq(
      (1L, "alice", "US"),
      (0L, "bob", "US"),   // raw_user_id=0 is the sentinel we want to filter
      (2L, "carol", "CA")
    ).toDF("raw_user_id", "raw_name", "raw_country")
    df.createOrReplaceTempView(viewName)
  }

  /** Flat SQL where the where clause references the raw column (pre-select name). */
  def testWheresOnRawColumnBeforeRename(): Unit = {
    makeView("test_raw_column")
    val sql = QueryUtils.build(
      selects = Map("user_id" -> "raw_user_id", "name" -> "raw_name"),
      from = "test_raw_column",
      wheres = Seq("raw_user_id > 0")  // raw name, not alias
    )
    val result = spark.sql(sql).collect()
    // Should keep alice (1) and carol (2), filter out bob (0)
    assert(result.length == 2, s"Expected 2 rows, got ${result.length}")
    val userIds = result.map(_.getAs[Long]("user_id")).toSet
    assert(userIds == Set(1L, 2L), s"Expected {1, 2}, got $userIds")
  }

  /** wheres referencing a column not in selects that exists upstream resolves via flat SQL. */
  def testWheresOnColumnAbsentFromSelects(): Unit = {
    makeView("test_absent_from_selects")
    val sql = QueryUtils.build(
      selects = Map("user_id" -> "raw_user_id", "name" -> "raw_name"),
      from = "test_absent_from_selects",
      wheres = Seq("raw_country = 'US'")  // raw_country is not in selects
    )
    val result = spark.sql(sql).collect()
    // Flat SQL: raw_country is a raw upstream column, resolves fine even though it's not projected
    assert(result.length == 2, s"Expected 2 rows (US only), got ${result.length}")
    val names = result.map(_.getAs[String]("name")).toSet
    assert(names == Set("alice", "bob"), s"Expected {alice, bob}, got $names")
  }

  /**
   * wheres referencing a select alias should fail with AnalysisException — alias is not visible
   * in WHERE in standard SQL (WHERE is evaluated before SELECT). This matches offline backfill
   * behavior and is the key semantic enforced by the flat-SQL approach.
   */
  def testWheresOnSelectAliasFails(): Unit = {
    makeView("test_alias_in_where")
    val sql = QueryUtils.build(
      selects = Map("user_id" -> "raw_user_id", "name" -> "raw_name"),
      from = "test_alias_in_where",
      wheres = Seq("user_id > 0")  // "user_id" is a select alias, not a raw column on the view
    )
    // Flat SQL: WHERE is evaluated before SELECT, so the alias "user_id" is not visible.
    // The view does not have a column named "user_id" — only "raw_user_id". Spark throws
    // AnalysisException. This is the correct behavior (parity with offline backfill).
    try {
      spark.sql(sql).collect()
      throw new AssertionError("Expected AnalysisException for alias reference in WHERE, but query succeeded")
    } catch {
      case _: org.apache.spark.sql.AnalysisException => // expected
    }
  }

  /** No selects (SELECT *): wheres still resolve against raw columns. */
  def testNoSelects(): Unit = {
    makeView("test_no_selects")
    val sql = QueryUtils.build(
      selects = null,
      from = "test_no_selects",
      wheres = Seq("raw_user_id > 0")
    )
    val result = spark.sql(sql).collect()
    assert(result.length == 2, s"Expected 2 rows with SELECT *, got ${result.length}")
  }

  /** No wheres: all rows pass through. */
  def testNoWheres(): Unit = {
    makeView("test_no_wheres")
    val sql = QueryUtils.build(
      selects = Map("user_id" -> "raw_user_id"),
      from = "test_no_wheres",
      wheres = Seq.empty
    )
    val result = spark.sql(sql).collect()
    assert(result.length == 3, s"Expected all 3 rows, got ${result.length}")
  }
}

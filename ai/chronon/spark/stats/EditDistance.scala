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

package ai.chronon.spark.stats

object EditDistance {

  // we re-use this object - hence the vars.
  case class Distance(var insert: Int, var delete: Int) {
    def total: Int = insert + delete
    def update(ins: Int, del: Int): Unit = {
      insert = ins
      delete = del
    }
  }

  // edit distance should also work for strings
  class StringOps(inner: String) extends Seq[Char] {
    override def length: Int = inner.length

    override def apply(idx: Int): Char = inner.charAt(idx)

    override def iterator: Iterator[Char] = inner.iterator
  }

  object StringOps {
    def from(value: String): Seq[Char] = if (value == null) null else new StringOps(value)
  }

  def betweenStrings(left: String, right: String): Distance = {
    between(StringOps.from(left), StringOps.from(right))
  }

  // EditDistance DP algorithm with the following tweaks
  //        - replacement is not allowed, only inserts or deletes
  //        - inserts and deletes need to be counted separately (distinguish missing online vs. extra online)
  //        - inserts and deletes are into right - to make it like left
  //        - early exits on nulls and empty cases (to avoid allocations)
  //        - minimize object creation - which is why Distance is not a case class
  //        - avoid scala for loops & ranges (scala's for loop overhead: https://github.com/scala/scala/pull/8069)
  def between(left: Any, right: Any): Distance = {
    // null & empty cases
    lazy val leftVals = left.asInstanceOf[Seq[Any]]
    lazy val rightVals = right.asInstanceOf[Seq[Any]]
    lazy val leftEmpty = left == null || leftVals.isEmpty
    lazy val rightEmpty = right == null || rightVals.isEmpty
    if (leftEmpty && rightEmpty) return Distance(0, 0)
    if (leftEmpty) return Distance(0, rightVals.length)
    if (rightEmpty) return Distance(leftVals.length, 0)

    // initialize (we don't create the whole nxm grid - we just create two)
    val editDistances0 = new Array[Distance](leftVals.length + 1)
    val editDistances1 = new Array[Distance](leftVals.length + 1)
    def editDistances(i: Int): Array[Distance] = if (i % 2 == 0) editDistances0 else editDistances1
    var i = 0
    while (i <= leftVals.length) {
      editDistances0.update(i, new Distance(i, 0))
      i += 1
    }

    // update i,j cell with inserts and deletes
    // to avoid allocating Distance objects, when not needed
    def update(i: Int, j: Int, inserts: Int, deletes: Int): Unit = {
      val arr = editDistances(i)
      val elem = arr(j)
      if (arr(j) == null) {
        arr.update(j, Distance(inserts, deletes))
      } else {
        elem.update(inserts, deletes)
      }
    }

    // recursive relation - on match - ed(i, j) = ed(i-1, j-1)
    //                    - else     - ed(i, j) = 1 + min(ed(i-1, j), ed(i , j-1))
    i = 1
    while (i <= rightVals.length) {
      var j = 0
      while (j <= leftVals.length) {
        if (j == 0) {
          update(i, j, 0, i)
        } else if (rightVals(i - 1) == leftVals(j - 1)) {
          val prev = editDistances(i - 1)(j - 1)
          update(i, j, prev.insert, prev.delete)
        } else {
          val deleteFromRight = editDistances(i - 1)(j)
          val insertIntoRight = editDistances(i)(j - 1)
          if (deleteFromRight.total < insertIntoRight.total) {
            update(i, j, deleteFromRight.insert, deleteFromRight.delete + 1)
          } else {
            update(i, j, insertIntoRight.insert + 1, insertIntoRight.delete)
          }
        }
        j += 1
      }
      i += 1
    }
    editDistances(rightVals.length)(leftVals.length)
  }
}

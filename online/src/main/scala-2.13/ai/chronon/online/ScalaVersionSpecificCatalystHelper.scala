package ai.chronon.online

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Predicate}

import scala.collection.Seq

object ScalaVersionSpecificCatalystHelper {

  def evalFilterExec(row: InternalRow, condition: Expression, attributes: Seq[Attribute]): Boolean = {
    val predicate = Predicate.create(condition, attributes.toSeq)
    predicate.initialize(0)
    val r = predicate.eval(row)
    r
  }
}

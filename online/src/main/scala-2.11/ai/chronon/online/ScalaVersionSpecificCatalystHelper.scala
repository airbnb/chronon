package ai.chronon.online

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InterpretedPredicate}

object ScalaVersionSpecificCatalystHelper {

  def evalFilterExec(row: InternalRow, condition: Expression, attributes: Seq[Attribute]): Boolean = {
    val predicate = InterpretedPredicate.create(condition, attributes)
    predicate.initialize(0)
    val r = predicate.eval(row)
    r
  }
}

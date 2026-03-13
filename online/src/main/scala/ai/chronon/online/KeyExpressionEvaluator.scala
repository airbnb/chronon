package ai.chronon.online

import ai.chronon.api.{StringType, StructField, StructType}

import java.util.concurrent.ConcurrentHashMap

object KeyExpressionEvaluator {

  private val cache = new ConcurrentHashMap[String, PooledCatalystUtil]()

  private def getOrCreate(sqlExpr: String, inputColumns: Seq[String]): PooledCatalystUtil = {
    val cacheKey = s"$sqlExpr|${inputColumns.sorted.mkString(",")}"
    cache.computeIfAbsent(
      cacheKey,
      new java.util.function.Function[String, PooledCatalystUtil] {
        override def apply(k: String): PooledCatalystUtil = {
          val inputSchema = StructType(
            "key_expr_input",
            inputColumns.map(col => StructField(col, StringType)).toArray
          )
          val expressions = Seq(("result", sqlExpr))
          new PooledCatalystUtil(expressions, inputSchema)
        }
      }
    )
  }

  def evaluate(sqlExpr: String, inputColumns: Seq[String], keys: Map[String, AnyRef]): AnyRef = {
    val util = getOrCreate(sqlExpr, inputColumns)
    val inputMap: Map[String, Any] = inputColumns.map { col =>
      col -> keys.getOrElse(col, null).asInstanceOf[Any]
    }.toMap
    val result = util.applyDerivations(inputMap)
    result
      .flatMap(_.get("result"))
      .map(_.asInstanceOf[AnyRef])
      .orNull
  }
}

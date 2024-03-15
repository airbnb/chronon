package ai.chronon.online

import ai.chronon.api.Extensions.DerivationOps
import ai.chronon.api.{Derivation, LongType, StringType, StructField, StructType}


object OnlineDerivationUtil {
  type DerivationFunc = (Map[String, Any], Map[String, Any]) => Map[String, Any]

  val timeFields: Array[StructField] = Array(
    StructField("ts", LongType),
    StructField("ds", StringType)
  )

  // remove value fields of groupBys that have failed with exceptions
  private[online] def reintroduceExceptions(derived: Map[String, Any], preDerivation: Map[String, Any]): Map[String, Any] = {
    val exceptions: Map[String, Any] = preDerivation.iterator.filter(_._1.endsWith("_exception")).toMap
    if (exceptions.isEmpty) {
      return derived
    }
    val exceptionParts: Array[String] = exceptions.keys.map(_.dropRight("_exception".length)).toArray
    derived.filterKeys(key => !exceptionParts.exists(key.startsWith)).toMap ++ exceptions
  }

  def buildRenameOnlyDerivationFunction(derivationsScala: List[Derivation]): DerivationFunc = {
    {
      case (_: Map[String, Any], values: Map[String, Any]) =>
        reintroduceExceptions(derivationsScala.applyRenameOnlyDerivation(values), values)
    }
  }

  def buildDerivationFunction(
    catalystUtil: PooledCatalystUtil
  ): DerivationFunc = {
    {
      case (keys: Map[String, Any], values: Map[String, Any]) =>
        reintroduceExceptions(catalystUtil.performSql(keys ++ values).orNull, values)
    }
  }

}

package ai.chronon.online

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api.Extensions.DerivationOps
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, expr}
import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.util.ScalaJavaConversions.ListOps

object DerivationUtils {

  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  type DerivationFunc = (Map[String, Any], Map[String, Any]) => Map[String, Any]

  val timeFields: Array[StructField] = Array(
    StructField("ts", LongType),
    StructField("ds", StringType)
  )

  // remove value fields of groupBys that have failed with exceptions
  // and reintroduce the exceptions back
  private[online] def reintroduceExceptions(derived: Map[String, Any],
                                            preDerivation: Map[String, Any]): Map[String, Any] = {
    val exceptions: Map[String, Any] = preDerivation.iterator.filter(_._1.endsWith("_exception")).toMap
    if (exceptions.isEmpty) {
      return derived
    }
    val exceptionParts: Array[String] = exceptions.keys.map(_.dropRight("_exception".length)).toArray
    derived.filterKeys(key => !exceptionParts.exists(key.startsWith)).toMap ++ exceptions
  }

  def buildRenameOnlyDerivationFunction(derivationsScala: List[Derivation]): DerivationFunc = {
    {
      case (keys: Map[String, Any], values: Map[String, Any]) =>
        reintroduceExceptions(derivationsScala.applyRenameOnlyDerivation(keys, values), values)
    }
  }

  private def buildDerivationFunctionWithSql(
      catalystUtil: PooledCatalystUtil
  ): DerivationFunc = {
    {
      case (keys: Map[String, Any], values: Map[String, Any]) =>
        reintroduceExceptions(catalystUtil.applyDerivations(keys ++ values).orNull, values)
    }
  }

  def buildDerivationFunction(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType
  ): DerivationFunc = {
    if (derivationsScala.isEmpty) {
      return { case (_, values: Map[String, Any]) => values }
    } else if (derivationsScala.areDerivationsRenameOnly) {
      buildRenameOnlyDerivationFunction(derivationsScala)
    } else {
      val catalystUtil = buildCatalystUtil(derivationsScala, keySchema, baseValueSchema)
      buildDerivationFunctionWithSql(catalystUtil)
    }
  }

  def applyDeriveFunc(
      deriveFunc: DerivationFunc,
      request: Request,
      baseMap: Map[String, AnyRef]
  ): Map[String, AnyRef] = {
    val requestTs = request.atMillis.getOrElse(System.currentTimeMillis())
    val requestDs = TsUtils.toStr(requestTs).substring(0, 10)
    // used for derivation based on ts/ds
    val tsDsMap: Map[String, AnyRef] = {
      Map("ts" -> (requestTs).asInstanceOf[AnyRef], "ds" -> (requestDs).asInstanceOf[AnyRef])
    }
    val derivedMap: Map[String, AnyRef] = deriveFunc(request.keys, baseMap ++ tsDsMap)
      .mapValues(_.asInstanceOf[AnyRef])
      .toMap
    val derivedMapCleaned = derivedMap -- tsDsMap.keys
    derivedMapCleaned
  }

  def buildCatalystUtil(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType
  ): PooledCatalystUtil = {
    val baseExpressions = if (derivationsScala.derivationsContainStar) {
      baseValueSchema
        .filterNot { derivationsScala.derivationExpressionSet contains _.name }
        .map(sf => sf.name -> sf.name)
    } else { Seq.empty }
    val expressions = baseExpressions ++ derivationsScala.derivationsWithoutStar.map { d => d.name -> d.expression }
    new PooledCatalystUtil(expressions, StructType("all", (keySchema ++ baseValueSchema).toArray ++ timeFields))
  }

  def buildDerivedFields(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType
  ): Seq[StructField] = {
    if (derivationsScala.areDerivationsRenameOnly) {
      val baseExpressions = if (derivationsScala.derivationsContainStar) {
        baseValueSchema.filterNot { derivationsScala.derivationExpressionSet contains _.name }
      } else {
        Seq.empty
      }
      val expressions: Seq[StructField] = baseExpressions ++ derivationsScala.derivationsWithoutStar.map { d =>
        {
          val allSchema = StructType("all", (keySchema ++ baseValueSchema).toArray)
          if (allSchema.typeOf(d.expression).isEmpty) {
            throw new IllegalArgumentException(
              s"Failed to run expression ${d.expression} for ${d.name}. Please ensure the derivation is " +
                s"correct.")
          } else {
            StructField(d.name, allSchema.typeOf(d.expression).get)
          }
        }
      }
      expressions
    } else {
      val catalystUtil = buildCatalystUtil(derivationsScala, keySchema, baseValueSchema)
      catalystUtil.outputChrononSchema.map(tup => StructField(tup._1, tup._2))
    }
  }

  def applyDerivation(joinConf: Join,
                      baseDf: DataFrame,
                      baseColumns: Seq[String],
                      leftColumns: Seq[String],
                      includeAllBase: Boolean = false): DataFrame = {
    if (!joinConf.isSetDerivations || joinConf.derivations.isEmpty) {
      return baseDf
    }

    logger.info(
      s"""Join ${joinConf.metaData.name} base schema:
         |${baseDf.schema.treeString}
         |""".stripMargin
    )

    val projections = joinConf.derivations.toScala.derivationProjection(baseColumns)
    val projectionsMap = projections.toMap
    val baseColumnsSet = baseDf.columns.toSet

    val finalOutputColumns =
      /*
       * Loop through all columns in the base join output:
       * 1. If it is one of the value columns, then skip it here and it will be handled later as we loop through
       *    derived columns again - derivation is a projection from all value columns to desired derived columns
       * 2.  (see case 2 below) If it is matching one of the projected output columns, then there are 2 sub-cases
       *     a. matching with a left column, then we handle the coalesce here to make sure left columns show on top
       *     b. a bootstrapped derivation case, the skip it here and it will be handled later as
       *        loop through derivations to perform coalescing
       * 3. Else, we keep it in the final output - cases falling here are either (1) key columns, or (2)
       *    arbitrary columns selected from left.
       */
      baseDf.columns.flatMap { c =>
        if (baseColumns.contains(c)) {
          None
        } else if (projectionsMap.contains(c)) {
          if (leftColumns.contains(c)) {
            Some(coalesce(col(c), expr(projectionsMap(c))).as(c))
          } else {
            None
          }
        } else {
          Some(col(c))
        }
      } ++
        /*
         * Loop through all clauses in derivation projections:
         * 1. (see case 2 above) If it is matching one of the projected output columns, then there are 2 sub-cases
         *     a. matching with a left column, then we skip since it is handled above
         *     b. a bootstrapped derivation case (see case 2 below), then we do the coalescing to achieve the bootstrap
         *        behavior.
         * 2. Else, we do the standard projection.
         */
        projections
          .flatMap {
            case (name, expression) =>
              if (baseColumnsSet.contains(name)) {
                if (leftColumns.contains(name)) {
                  None
                } else {
                  // Happens when derivation overrides an existing base column name
                  Some(coalesce(col(name), expr(expression)).as(name))
                }
              } else {
                Some(expr(expression).as(name))
              }
          } ++
        /*
         * Include all base columns that is not already selected, if includeAllBase is true:
         * - Include it again unless it is a duplicate from left columns (rare) or handled by derivation (either
         * rename or because of select-*)
         * - This is because rename will remove base columns but here we want to retain it.
         */ {
          if (!includeAllBase) {
            Seq()
          } else {
            baseColumns.filterNot(c => leftColumns.contains(c) || projectionsMap.contains(c)).map(col)
          }
        }

    logger.info(s"Join ${joinConf.metaData.name} derivation logic: \n-- ${finalOutputColumns.mkString("\n-- ")}")

    val result = baseDf.select(finalOutputColumns: _*)

    logger.info(
      s"""Join ${joinConf.metaData.name} derived schema:
         |${result.schema.treeString}
         |""".stripMargin
    )
    result
  }
}

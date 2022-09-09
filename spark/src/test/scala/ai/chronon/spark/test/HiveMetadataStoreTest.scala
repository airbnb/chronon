package ai.chronon.spark.test

import ai.chronon.spark.{HiveMetadataStore, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Test


class HiveMetadataStoreTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("HiveMetadataStoreTest", local = true)
  private val hiveMetadataStore = new HiveMetadataStore(spark)
  private val tableUtils = TableUtils(spark)

  @Test
  def getHiveMetadataTest(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS hive_test")
    val tableName = "hive_test." + hiveMetadataStore.MetadataStoreTableName
    //clean up table if already exists
    tableUtils.sql(s"DROP TABLE IF EXISTS ${tableName}")

    //create test table
    val metadataSchema = StructType( Array(
      StructField("conf_key", StringType,true),
      StructField("entity_type", StringType,true),
      StructField("schema", StringType,true),
      StructField("schema_hash", StringType,true),
      StructField("created_at", TimestampType,true)
    ))
    val createMetaTable = tableUtils.createTableSql(tableName, metadataSchema,
      Seq.empty[String], null, "parquet")
    tableUtils.sql(createMetaTable)
    println(s"Metadata schema table ${tableName} created")

    val confKey = "joins/testConfKey"
    val entityType = "join"
    val schemaHash = "testHash"
    val schema = StructType( Array(
      StructField("item", StringType,true),
      StructField("item_view_avg", DoubleType,true)
    ))

    hiveMetadataStore.uploadEntityMetadata(confKey, schema, schemaHash, entityType, tableName)
    val schemaEntry = hiveMetadataStore.getEntityMetadata(confKey, schemaHash, tableName)
    assert(!schemaEntry.isEmpty)
    assert(schemaEntry.first().get(0).equals(confKey))
    assert(schemaEntry.first().get(3).equals(schemaHash))

    hiveMetadataStore.uploadEntityMetadata(confKey, schema, "latestHash", entityType, tableName)
    val latestSchemaEntry = hiveMetadataStore.getEntityMetadata(confKey, "", tableName)
    assert(latestSchemaEntry.first().get(0).equals(confKey))
    assert(latestSchemaEntry.first().get(3).equals("latestHash"))

    // batch get
    val confList = Range.Int.inclusive(1,5,1).map("key" + _)
    val hashList = confList.map(_ + "_hash") ++ confList.map(_ + "_latest_hash")
    val entries = (confList ++ confList).zip(hashList)
    entries.map(entry => hiveMetadataStore.uploadEntityMetadata(entry._1, schema, entry._2, "join", tableName))
    val batchGetResult = hiveMetadataStore.batchGetEntityMetadata(confList.toList, tableName)
    //assert hash == key_latest_hash
    batchGetResult.foreach(r => assert(r.first().get(3).equals(r.first().get(0) + "_latest_hash")))

    // clean up
    tableUtils.sql(s"DROP TABLE IF EXISTS ${tableName}")
  }
}

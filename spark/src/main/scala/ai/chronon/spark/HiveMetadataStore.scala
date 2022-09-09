package ai.chronon.spark

import com.google.gson.Gson
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

class HiveMetadataStore(sparkSession: SparkSession) {
  private val CreatedAtColumnName = "created_at"
  private val ConfKeyColumnName = "conf_key"
  private val SchemaHashColumnName = "schema_hash"
  private val tableUtils = TableUtils(sparkSession)
  val MetadataStoreTableName = "chronon_metadata_store"

  sparkSession.sparkContext.setLogLevel("ERROR")

  // Test only method. Upload operation will be completed by batch scheduled job
  def uploadEntityMetadata(confKey: String,
                           schema: StructType,
                           schemaHash: String,
                           entityType: String = "None",
                           metaTableName: String = MetadataStoreTableName
                          ): Unit = {
    val serializedSchema = new Gson().toJson(schema)
    val createdAt = new Timestamp(System.currentTimeMillis())
    if (sparkSession.catalog.tableExists(metaTableName)) {
      val insertSql = s"INSERT INTO TABLE ${metaTableName} VALUES " +
        s"('$confKey', '$entityType', '$serializedSchema', '$schemaHash', '$createdAt')"
      tableUtils.sql(insertSql)
    } else {
      println(s"${metaTableName} doesn't exist, please double check before inserting entry")
    }
  }

  def getEntityMetadata(confKey: String, schemaHash: String = "",
                        metaTableName: String = MetadataStoreTableName): DataFrame = {
    var getSql: String = ""
    if(schemaHash.isEmpty ) {
      getSql = s"SELECT * FROM ${metaTableName} WHERE ${ConfKeyColumnName} = '$confKey' order by ${CreatedAtColumnName} DESC limit 1"
    } else {
      getSql = s"SELECT * FROM ${metaTableName} WHERE ${ConfKeyColumnName} = '$confKey' and ${SchemaHashColumnName} = '$schemaHash'"
    }
    tableUtils.sql(getSql)
  }

  def batchGetEntityMetadata(confKeys: List[String],
                             metaTableName: String = MetadataStoreTableName): List[DataFrame] = {
    confKeys.par.map(cf => getEntityMetadata(cf, metaTableName = metaTableName)).toList
  }

}

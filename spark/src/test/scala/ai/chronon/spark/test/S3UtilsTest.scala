package ai.chronon.spark.test

import ai.chronon.spark._
import org.junit.Assert.assertEquals
import org.junit.Test

class S3UtilsTest {

  @Test
  def testUpdateS3Prefix(): Unit = {
    assertEquals(S3Utils.updateS3Prefix("s3://my/table/{{ds_nodash}}", "20240201"), "s3://my/table/20240202")
    assertEquals(S3Utils.updateS3Prefix("s3://my/table/{{yesterday_ds_nodash}}", "20240201"), "s3://my/table/20240201")
  }
}

package ai.chronon.spark.test

import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.MetadataEndPoint.NameByTeamEndPointName
import ai.chronon.online.{MetadataDirWalker, MetadataEndPoint, MetadataStore}
import junit.framework.TestCase
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

class MetadataStoreTest extends TestCase {
  val joinPath = "joins/team/example_join.v1"
  val confResourcePath = ExampleDataUtils.getExampleData(joinPath)
  val src = Source.fromFile(confResourcePath)

  val expected = {
    try src.mkString
    finally src.close()
  }.replaceAll("\\s+", "")

  val acceptedEndPoints = List(MetadataEndPoint.ConfByKeyEndPointName, MetadataEndPoint.NameByTeamEndPointName)

  @Test
  def testMetadataStoreSingleFile(): Unit = {
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val singleFileDataSet = ChrononMetadataKey
    val singleFileMetaDataSet = NameByTeamEndPointName
    val singleFileMetadataStore = new MetadataStore(inMemoryKvStore, singleFileDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(singleFileDataSet)
    inMemoryKvStore.create(NameByTeamEndPointName)
    // set the working directory to /chronon instead of $MODULE_DIR in configuration if Intellij fails testing
    val singleFileDirWalker = new MetadataDirWalker(confResourcePath, acceptedEndPoints)
    val singleFileKvMap = singleFileDirWalker.run
    val singleFilePut: Seq[Future[scala.collection.Seq[Boolean]]] = singleFileKvMap.toSeq.map {
      case (endPoint, kvMap) => singleFileMetadataStore.put(kvMap, endPoint)
    }
    singleFilePut.flatMap(putRequests => Await.result(putRequests, Duration.Inf))

    val response = inMemoryKvStore.get(GetRequest(joinPath.getBytes(), singleFileDataSet))
    val res = Await.result(response, Duration.Inf)
    assertTrue(res.latest.isSuccess)
    val actual = new String(res.values.get.head.bytes)
    assertEquals(expected, actual.replaceAll("\\s+", ""))

    val teamMetadataResponse = inMemoryKvStore.getStringArray("joins/relevance", singleFileMetaDataSet, 10000)
    assert(teamMetadataResponse.get.length == 1)
    assert(teamMetadataResponse.get.contains("joins/team/example_join.v1"))
    assertTrue(singleFileMetadataStore.validateJoinExist("relevance", "team/example_join.v1"))
    assertFalse(singleFileMetadataStore.validateJoinExist("team", "team/example_join.v1"))

    val emptyResponse =
      inMemoryKvStore.get(GetRequest("NoneExistKey".getBytes(), "NonExistDataSetName"))
    val emptyRes = Await.result(emptyResponse, Duration.Inf)
    assertFalse(emptyRes.latest.isSuccess)
  }

  @Test
  def testMetadataStoreDirectory(): Unit = {
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val directoryDataSetDataSet = ChrononMetadataKey
    val directoryMetadataDataSet = NameByTeamEndPointName
    val directoryMetadataStore = new MetadataStore(inMemoryKvStore, directoryDataSetDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(directoryDataSetDataSet)
    inMemoryKvStore.create(directoryMetadataDataSet)
    val directoryDataDirWalker =
      new MetadataDirWalker(confResourcePath.replace(s"/$joinPath", ""), acceptedEndPoints)
    val directoryDataKvMap = directoryDataDirWalker.run
    val directoryPut = directoryDataKvMap.toSeq.map {
      case (endPoint, kvMap) => directoryMetadataStore.put(kvMap, endPoint)
    }
    directoryPut.flatMap(putRequests => Await.result(putRequests, Duration.Inf))
    val dirResponse =
      inMemoryKvStore.get(GetRequest(joinPath.getBytes(), directoryDataSetDataSet))
    val dirRes = Await.result(dirResponse, Duration.Inf)
    assertTrue(dirRes.latest.isSuccess)
    val dirActual = new String(dirRes.values.get.head.bytes)
    assertEquals(expected, dirActual.replaceAll("\\s+", ""))

    val teamMetadataDirResponse = inMemoryKvStore.getStringArray("group_bys/team", directoryMetadataDataSet, 10000)
    assert(teamMetadataDirResponse.get.length == 1)
    val teamMetadataDirRes = teamMetadataDirResponse.get.head
    assert(teamMetadataDirRes.equals("group_bys/team/example_group_by.v1"))

    assertFalse(directoryMetadataStore.validateGroupByExist("relevance", "team/example_group_by.v1"))
    assertTrue(directoryMetadataStore.validateGroupByExist("team", "team/example_group_by.v1"))
  }

}

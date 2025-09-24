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

package ai.chronon.online.test

import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.{ExternalPart, ExternalSource, ExternalSourceFactoryConfig, Join, MetaData}
import ai.chronon.online.{
  Api,
  ExternalSourceFactory,
  ExternalSourceHandler,
  ExternalSourceRegistry,
  Fetcher,
  GroupByServingInfoParsed,
  KVStore,
  LoggableResponse,
  StreamDecoder,
  TTLCache
}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ExternalSourceFactoryTest {

  // Mock implementations for testing
  class MockExternalSourceFactory extends ExternalSourceFactory {
    override def createExternalSourceHandler(externalSource: ExternalSource): ExternalSourceHandler = {
      new ExternalSourceHandler {
        override def fetch(requests: scala.collection.Seq[Fetcher.Request])
            : concurrent.Future[scala.collection.Seq[Fetcher.Response]] = {
          concurrent.Future.successful(Seq.empty)
        }
      }
    }
  }

  class TestApi(userConf: Map[String, String]) extends Api(userConf) {
    private val mockKvStore = mock(classOf[KVStore])
    private val testRegistry = new ExternalSourceRegistry()

    // Map to store mock join configurations for testing
    private var mockJoinConfigs = Map.empty[String, JoinOps]
    private var joinConfigExceptions = Map.empty[String, Exception]

    // Add a test factory to the registry
    testRegistry.addFactory("test-factory", new MockExternalSourceFactory())

    override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = {
      mock(classOf[StreamDecoder])
    }

    override def genKvStore: KVStore = mockKvStore

    override def externalRegistry: ExternalSourceRegistry = testRegistry

    override def logResponse(resp: LoggableResponse): Unit = {}

    // Override fetcher to return a mock that's initialized on-demand
    override lazy val fetcher: Fetcher = {
      val mockFetcher = mock(classOf[Fetcher])

      // Create a mock TTLCache for getJoinConf
      val mockTTLCache = mock(classOf[TTLCache[String, Try[JoinOps]]])

      // Set up mock behaviors for successful configs
      mockJoinConfigs.foreach {
        case (joinName, joinOps) =>
          when(mockTTLCache.apply(joinName)).thenReturn(Success(joinOps))
      }

      // Set up mock behaviors for failed configs
      joinConfigExceptions.foreach {
        case (joinName, exception) =>
          when(mockTTLCache.apply(joinName)).thenReturn(Failure(exception))
      }

      // Wire the TTLCache to the fetcher
      when(mockFetcher.getJoinConf).thenReturn(mockTTLCache)
      mockFetcher
    }

    // Helper method to set up mock join configurations for testing
    def setMockJoinConfig(joinName: String, joinOps: JoinOps): Unit = {
      mockJoinConfigs = mockJoinConfigs + (joinName -> joinOps)
      // The mock behavior will be set up when the fetcher lazy val is accessed
    }

    // Helper method to simulate join config loading failure
    def setJoinConfigException(joinName: String, exception: Exception): Unit = {
      joinConfigExceptions = joinConfigExceptions + (joinName -> exception)
      // The mock behavior will be set up when the fetcher lazy val is accessed
    }
  }

  private def createMockJoin(joinName: String, externalSourceName: String, factoryName: String = null): Join = {
    val join = mock(classOf[Join])
    val externalPart = mock(classOf[ExternalPart])
    val externalSource = mock(classOf[ExternalSource])
    val metadata = mock(classOf[MetaData])

    when(metadata.getName).thenReturn(externalSourceName)
    when(externalSource.getMetadata).thenReturn(metadata)

    // Handle factory configuration mocking based on factoryName
    if (factoryName == null) {
      // No factory configuration
      when(externalSource.getFactoryConfig).thenReturn(null)
    } else {
      // Create mock factory config with factory name and params
      val factoryConfig = mock(classOf[ExternalSourceFactoryConfig])
      val factoryParams = mock(classOf[java.util.Map[String, String]])
      when(factoryConfig.getFactoryName).thenReturn(factoryName)
      when(factoryConfig.getFactoryParams).thenReturn(factoryParams)
      when(externalSource.getFactoryConfig).thenReturn(factoryConfig)
    }

    when(externalPart.getSource).thenReturn(externalSource)
    when(join.getOnlineExternalParts).thenReturn(Seq(externalPart).asJava)

    join
  }

  private def createMockJoinOps(join: Join): JoinOps = {
    val joinOps = mock(classOf[JoinOps])
    when(joinOps.join).thenReturn(join)
    joinOps
  }

  @Test
  def testRegisterExternalSourcesWithValidFactory(): Unit = {
    val api = new TestApi(Map.empty)

    // Create mock join with factory configuration
    val join = createMockJoin("test-join", "test-source", "test-factory")
    val joinOps = createMockJoinOps(join)

    // Set up mock join config
    api.setMockJoinConfig("test-join", joinOps)

    // Execute the method
    api.registerJoinExternalSources(Seq("test-join"))

    // Verify that the handler was registered
    assertTrue("Handler should be registered", api.externalRegistry.handlerMap.contains("test-source"))
  }

  @Test
  def testRegisterExternalSourcesWithMissingFactory(): Unit = {
    val api = new TestApi(Map.empty)

    // Create mock join with non-existent factory
    val join = createMockJoin("test-join", "test-source", "non-existent-factory")
    val joinOps = createMockJoinOps(join)

    // Set up mock join config
    api.setMockJoinConfig("test-join", joinOps)

    // Execute and expect exception
    val exception = intercept[IllegalArgumentException] {
      api.registerJoinExternalSources(Seq("test-join"))
    }

    assertTrue("Exception should mention missing factory",
               exception.getMessage.contains("Factory 'non-existent-factory' is not registered"))
    assertTrue("Exception should list available factories",
               exception.getMessage.contains("Available factories: [test-factory]"))
  }

  @Test
  def testRegisterExternalSourcesWithFailedJoinConfig(): Unit = {
    val api = new TestApi(Map.empty)

    val originalException = new RuntimeException("Join config not found")

    // Set up exception to be thrown by fetcher.getJoinConf
    api.setJoinConfigException("test-join", originalException)

    // Execute and expect exception
    val thrownException = intercept[RuntimeException] {
      api.registerJoinExternalSources(Seq("test-join"))
    }

    assertEquals("Exception should wrap original exception", originalException, thrownException.getCause)
    assertTrue("Exception message should mention failed join config",
               thrownException.getMessage.contains("Failed to load join configuration for 'test-join'"))
  }

  @Test
  def testRegisterExternalSourcesWithNoExternalParts(): Unit = {
    val api = new TestApi(Map.empty)

    // Record initial handler count (includes default contextual handler)
    val initialHandlerCount = api.externalRegistry.handlerMap.size

    // Create mock join without external parts
    val join = mock(classOf[Join])
    when(join.getOnlineExternalParts).thenReturn(null)
    val joinOps = createMockJoinOps(join)

    // Set up mock join config
    api.setMockJoinConfig("test-join", joinOps)

    // Execute the method - should not throw exception
    api.registerJoinExternalSources(Seq("test-join"))

    // Should complete successfully without registering any new handlers
    assertEquals("No new handlers should be registered", initialHandlerCount, api.externalRegistry.handlerMap.size)
  }

  @Test
  def testRegisterExternalSourcesWithNullFactoryName(): Unit = {
    val api = new TestApi(Map.empty)

    // Create mock join with factory config but null factory name
    val join = mock(classOf[Join])
    val externalPart = mock(classOf[ExternalPart])
    val externalSource = mock(classOf[ExternalSource])
    val metadata = mock(classOf[MetaData])
    val factoryConfig = mock(classOf[ExternalSourceFactoryConfig])

    when(metadata.getName).thenReturn("test-source")
    when(externalSource.getMetadata).thenReturn(metadata)
    when(externalSource.getFactoryConfig).thenReturn(factoryConfig)
    when(factoryConfig.getFactoryName).thenReturn(null) // null factory name
    when(externalPart.getSource).thenReturn(externalSource)
    when(join.getOnlineExternalParts).thenReturn(Seq(externalPart).asJava)

    val joinOps = createMockJoinOps(join)
    api.setMockJoinConfig("test-join", joinOps)

    // Execute and expect exception
    val exception = intercept[IllegalArgumentException] {
      api.registerJoinExternalSources(Seq("test-join"))
    }

    assertTrue("Exception should mention null factoryName", exception.getMessage.contains("factoryName is null"))
  }
}

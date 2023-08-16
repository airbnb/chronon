package ai.chronon.spark.test;

import ai.chronon.api.DataType;
import ai.chronon.api.DoubleType$;
import ai.chronon.api.IntType$;
import ai.chronon.api.Join;
import ai.chronon.api.ListType;
import ai.chronon.api.LongType$;
import ai.chronon.api.MapType;
import ai.chronon.api.StringType$;
import ai.chronon.online.Api;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.Fetcher;
import ai.chronon.spark.TableUtils;
import ai.chronon.spark.SparkSessionBuilder;
import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static scala.compat.java8.JFunction.func;

public class JavaFetcherTest {
    String namespace = "java_fetcher_test";
    SparkSession session = SparkSessionBuilder.build(namespace, true, scala.Option.apply(null), scala.Option.apply(null));
    TableUtils tu = new TableUtils(session);
    InMemoryKvStore kvStore = new InMemoryKvStore(func(() -> tu));
    MockApi mockApi = new MockApi(func(() -> kvStore), "java_fetcher_test");
    JavaFetcher fetcher = mockApi.buildJavaFetcher();

    @Test
    public void testException() throws InterruptedException, ExecutionException, TimeoutException {
        List<JavaRequest> requests = new ArrayList<>();
        requests.add(new JavaRequest("non_existent", null));
        CompletableFuture<List<JavaResponse>> responsesF = fetcher.fetchGroupBys(requests);
        responsesF.exceptionally(e -> {
            System.out.println("Caught internal exception " + e.getMessage());
            return null;
        });
        List<JavaResponse> responses = responsesF.get(10000, TimeUnit.MILLISECONDS);
        Gson gson = new Gson();
        String responseValues = gson.toJson(responses.get(0).values);
        System.out.println(responseValues);
        assertFalse(responses.get(0).values.isSuccess());
    }

    @Test
    public void testRetrieveSchema() {
        Join generatedJoin = TestUtils.generateRandomData(session, namespace, 10, 10, "test_topic_java");

        Api mockApi = TestUtils.setupFetcherWithJoin(session, generatedJoin, namespace);
        JavaFetcher javaFetcher = mockApi.buildJavaFetcher();

        Map<String, DataType> joinSchemaResult = javaFetcher.retrieveJoinSchema(generatedJoin.getMetaData().getName());
        assertEquals(joinSchemaResult, JavaConverters.mapAsJavaMap(TestUtils.expectedSchemaForTestPaymentsJoin()));

        String groupByName = "unit_test/vendor_ratings";
        assertTrue(generatedJoin.joinParts.stream().map(j -> j.groupBy.getMetaData().getName()).collect(Collectors.toSet()).contains(groupByName));
        Map<String, DataType> groupBySchemaResult = javaFetcher.retrieveGroupBySchema(groupByName);
        assertEquals(groupBySchemaResult, JavaConverters.mapAsJavaMap(TestUtils.expectedSchemaForVendorRatingsGroupBy()));
    }


    @Test
    public void testNullMapConversion() throws InterruptedException, ExecutionException, TimeoutException {
        List<JavaRequest> requests = new ArrayList<>();
        requests.add(new JavaRequest("non_existent", null));

        // can end up with a null result response if the GroupBy is not found
        List<JavaResponse> nullResultResponses = new ArrayList<>();
        Fetcher.Response nullScalaResponse = new Fetcher.Response(
                requests.get(0).toScalaRequest(),
                new scala.util.Success<>(null));
        nullResultResponses.add(new JavaResponse(nullScalaResponse));

        CompletableFuture<List<JavaResponse>> responsesF = CompletableFuture.completedFuture(nullResultResponses);
        List<JavaResponse> responses = responsesF.get(10000, TimeUnit.MILLISECONDS);
        Gson gson = new Gson();
        String responseValues = gson.toJson(responses.get(0).values);
        System.out.println(responseValues);
        assertTrue(responses.get(0).values.isSuccess());
    }
}

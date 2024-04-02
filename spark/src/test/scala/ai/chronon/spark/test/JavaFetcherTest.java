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

package ai.chronon.spark.test;

import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.Fetcher;
import ai.chronon.spark.TableUtils;
import ai.chronon.spark.SparkSessionBuilder;
import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static scala.compat.java8.JFunction.func;

public class JavaFetcherTest {
    String namespace = "java_fetcher_test";
    SparkSession session = SparkSessionBuilder.build(namespace, true, scala.Option.apply(null), scala.Option.apply(null), true);
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

package ai.zipline.spark.test;

import ai.zipline.online.JavaFetcher;
import ai.zipline.online.JavaRequest;
import ai.zipline.online.JavaResponse;
import ai.zipline.online.KVStore;
import ai.zipline.spark.SparkSessionBuilder;
import ai.zipline.spark.TableUtils;
import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Function0;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static scala.compat.java8.JFunction.func;

public class JavaFetcherTest {
    String namespace = "java_fetcher_test";
    SparkSession session = SparkSessionBuilder.build(namespace, true);
    TableUtils tu = new TableUtils(session);
    InMemoryKvStore kvStore = new InMemoryKvStore(func(() -> tu));
    // MockApi mockApi = new MockApi(func(() -> kvStore));
    JavaFetcher fetcher = new JavaFetcher(kvStore);

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
    }
}

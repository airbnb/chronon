package ai.chronon.online;

import ai.chronon.api.Constants;
import ai.chronon.online.Fetcher.Request;
import ai.chronon.online.Fetcher.Response;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class JavaFetcher  {
  Fetcher fetcher;

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry);
  }

  public static List<JavaResponse> toJavaResponses(Seq<Response> responseSeq) {
    List<JavaResponse> result = new ArrayList<>(responseSeq.size());
    Iterator<Response> it = responseSeq.iterator();
    while(it.hasNext()) {
      result.add(new JavaResponse(it.next()));
    }
    return result;
  }

  public static JavaResponse toJavaResponse(Response response) {
    return new JavaResponse(response);
  }

  private CompletableFuture<List<JavaResponse>> convertResponses(Future<Seq<Response>> responses) {
    return FutureConverters
        .toJava(responses)
        .toCompletableFuture()
        .thenApply(JavaFetcher::toJavaResponses);
  }

  private Seq<Request> convertJavaRequestList(List<JavaRequest> requests) {
    ArrayBuffer<Request> scalaRequests = new ArrayBuffer<>();
    for (JavaRequest request : requests) {
      Request convertedRequest = request.toScalaRequest();
      scalaRequests.$plus$eq(convertedRequest);
    }
    return scalaRequests.toSeq();
  }

  public CompletableFuture<List<JavaResponse>> fetchGroupBys(List<JavaRequest> requests) {
    // Get responses from the fetcher
    Future<Seq<Response>> responses = this.fetcher.fetchGroupBys(convertJavaRequestList(requests));
    // Convert responses to CompletableFuture
    return convertResponses(responses);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoin(List<JavaRequest> requests) {
    Future<Seq<Response>> responses = this.fetcher.fetchJoin(convertJavaRequestList(requests));
    // Convert responses to CompletableFuture
    return convertResponses(responses);
  }

  public CompletableFuture<List<JavaResponse>> fetchStats(JavaStatsRequest request) {
    Future<Seq<Response>> responses = this.fetcher.fetchStats(request.toScalaRequest());
    // Convert responses to CompletableFuture
    return convertResponses(responses);
  }

  public CompletableFuture<JavaResponse> fetchMergedStatsBetween(JavaStatsRequest request) {
    Future<Response> response = this.fetcher.fetchMergedStatsBetween(request.toScalaRequest());
    // Convert responses to CompletableFuture
    return FutureConverters.toJava(response).toCompletableFuture().thenApply(JavaFetcher::toJavaResponse);
  }

}

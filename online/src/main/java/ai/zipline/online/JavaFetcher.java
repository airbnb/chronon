package ai.zipline.online;

import ai.zipline.online.KVStore;
import ai.zipline.api.Constants;
import ai.zipline.online.Fetcher;
import ai.zipline.online.Fetcher.Request;
import ai.zipline.online.Fetcher.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.CollectionConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.concurrent.Future;
import scala.compat.java8.FutureConverters;

public class JavaFetcher  {
  public static final Long DEFAULT_TIMEOUT = 10000L;
  Fetcher fetcher;

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis);
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet) {
    this(kvStore, metaDataSet, DEFAULT_TIMEOUT);
  }

  public JavaFetcher(KVStore kvStore, Long timeoutMillis) {
    this(kvStore, Constants.ZiplineMetadataKey(), timeoutMillis);
  }

  public JavaFetcher(KVStore kvStore) {
    this(kvStore, Constants.ZiplineMetadataKey());
  }

  public static List<JavaResponse> toJavaResponses(Seq<Response> responseSeq) {
    List<JavaResponse> result = new ArrayList<>(responseSeq.size());
    Iterator<Response> it = responseSeq.iterator();
    while(it.hasNext()) {
      result.add(new JavaResponse(it.next()));
    }
    return result;
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
    Future<Seq<Response>> responses = this.fetcher.fetchGroupBys(convertJavaRequestList(requests), scala.Option.apply(null));
    // Convert responses to CompletableFuture
    return convertResponses(responses);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoin(List<JavaRequest> requests) {
    Future<Seq<Response>> responses = this.fetcher.fetchJoin(convertJavaRequestList(requests));
    // Convert responses to CompletableFuture
    return convertResponses(responses);
  }

}

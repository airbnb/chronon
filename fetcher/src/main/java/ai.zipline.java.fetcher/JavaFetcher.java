package ai.zipline.java.fetcher;

import ai.zipline.api.KVStore;
import ai.zipline.fetcher.Fetcher;
import ai.zipline.fetcher.Fetcher.Request;
import ai.zipline.fetcher.Fetcher.Response;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.compat.java8.FutureConverters;

public class JavaFetcher  {
  public static final String DEFAULT_METADATA_SET = "ZIPLINE_METADATA";
  public static final Long DEFAULT_TIMEOUT = new Long(1000);
  Fetcher fetcher;

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis);
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet) {
    this(kvStore, metaDataSet, DEFAULT_TIMEOUT);
  }

  public JavaFetcher(KVStore kvStore, Long timeoutMillis) {
    this(kvStore, DEFAULT_METADATA_SET, timeoutMillis);
  }

  public JavaFetcher(KVStore kvStore) {
    this(kvStore, DEFAULT_METADATA_SET);
  }

  private Seq<Request> ConvertRequests(List<Request> requests) {
    return JavaConverters.asScalaIteratorConverter(requests.iterator()).asScala().toSeq();
  }

  public List<Response> convertSeqResponse(Seq<Response> scalaResponse) {
    return scala.collection.JavaConversions.seqAsJavaList(scalaResponse);
  }

  private CompletableFuture<List<Response>> ConvertResponses(Future<Seq<Response>> responses) {
    return FutureConverters
        .toJava(responses)
        .toCompletableFuture()
        .thenApply(response -> scala.collection.JavaConversions.seqAsJavaList(response));
  }

  public CompletableFuture<List<Response>> fetchGroupBys(List<Request> requests) {
    // Get responses from the fetcher
    Future<Seq<Response>> responses = this.fetcher.fetchGroupBys(ConvertRequests(requests));
    // Convert responses to CompletableFuture
    return ConvertResponses(responses);
  }

  public CompletableFuture<List<Response>> fetchJoin(List<Request> requests) {
    // Get responses from the fetcher
    Future<Seq<Response>> responses = this.fetcher.fetchJoin(ConvertRequests(requests));
    // Convert responses to CompletableFuture
    return ConvertResponses(responses);
  }

}

package ai.chronon.online;

import ai.chronon.api.DataType;
import ai.chronon.api.GroupByServingInfo;
import ai.chronon.api.Join;
import ai.chronon.api.StructField;
import ai.chronon.api.Constants;
import ai.chronon.online.Fetcher.Request;
import ai.chronon.online.Fetcher.Response;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.concurrent.JavaConversions;
import scala.collection.JavaConverters;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Map.*;

public class JavaFetcher {
  Fetcher fetcher;

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry);
  }

  public static List<JavaResponse> toJavaResponses(Seq<Response> responseSeq) {
    List<JavaResponse> result = new ArrayList<>(responseSeq.size());
    Iterator<Response> it = responseSeq.iterator();
    while (it.hasNext()) {
      result.add(new JavaResponse(it.next()));
    }
    return result;
  }
  
  private CompletableFuture<List<JavaResponse>> convertResponsesWithTs(Future<FetcherResponseWithTs> responses, boolean isGroupBy, long startTs) {
    return FutureConverters.toJava(responses).toCompletableFuture().thenApply(resps -> {
      List<JavaResponse> jResps = toJavaResponses(resps.responses());
      List<String> requestNames = jResps.stream().map(jResp -> jResp.request.name).collect(Collectors.toList());
      instrument(requestNames, isGroupBy, "java.response_conversion.latency.millis", resps.endTs());
      instrument(requestNames, isGroupBy, "java.overall.latency.millis", startTs);
      return jResps;
    });
  }

  public static List<JavaStatsResponse> toJavaStatsResponses(Seq<Fetcher.StatsResponse> responseSeq) {
    List<JavaStatsResponse> result = new ArrayList<>(responseSeq.size());
    Iterator<Fetcher.StatsResponse> it = responseSeq.iterator();
    while(it.hasNext()) {
      result.add(new JavaStatsResponse(it.next()));
    }
    return result;
  }

  public static JavaStatsResponse toJavaStatsResponse(Fetcher.StatsResponse response) {
    return new JavaStatsResponse(response);
  }
  public static JavaMergedStatsResponse toJavaMergedStatsResponse(Fetcher.MergedStatsResponse response) {
    return new JavaMergedStatsResponse(response);
  }
  public static JavaSeriesStatsResponse toJavaSeriesStatsResponse(Fetcher.SeriesStatsResponse response) {
    return new JavaSeriesStatsResponse(response);
  }

  private CompletableFuture<List<JavaStatsResponse>> convertStatsResponses(Future<Seq<Fetcher.StatsResponse>> responses) {
    return FutureConverters
            .toJava(responses)
            .toCompletableFuture()
            .thenApply(JavaFetcher::toJavaStatsResponses);
  }

  private Seq<Request> convertJavaRequestList(List<JavaRequest> requests, boolean isGroupBy, long startTs) {
    ArrayBuffer<Request> scalaRequests = new ArrayBuffer<>();
    for (JavaRequest request : requests) {
      Request convertedRequest = request.toScalaRequest();
      scalaRequests.$plus$eq(convertedRequest);
    }
    Seq<Request> scalaRequestsSeq = scalaRequests.toSeq();
    instrument(requests.stream().map(jReq -> jReq.name).collect(Collectors.toList()), isGroupBy, "java.request_conversion.latency.millis", startTs);
    return scalaRequestsSeq;
  }

  public CompletableFuture<List<JavaResponse>> fetchGroupBys(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    Seq<Request> scalaRequests = convertJavaRequestList(requests, true, startTs);
    // Get responses from the fetcher
    Future<FetcherResponseWithTs> scalaResponses = this.fetcher.withTs(this.fetcher.fetchGroupBys(scalaRequests));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, true, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoin(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    Seq<Request> scalaRequests = convertJavaRequestList(requests, false, startTs);
    // Get responses from the fetcher
    Future<FetcherResponseWithTs> scalaResponses = this.fetcher.withTs(this.fetcher.fetchJoin(scalaRequests));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, false, startTs);
  }

  private void instrument(List<String> requestNames, boolean isGroupBy, String metricName, Long startTs) {
    long endTs = System.currentTimeMillis();
    for (String s : requestNames) {
      Metrics.Context ctx;
      if (isGroupBy) {
        ctx = getGroupByContext(s);
      } else {
        ctx = getJoinContext(s);
      }
      ctx.histogram(metricName, endTs - startTs);
    }
  }

  private Metrics.Context getJoinContext(String joinName) {
    return new Metrics.Context("join.fetch", joinName, null, null, false, null, null, null, null);
  }

  private Metrics.Context getGroupByContext(String groupByName) {
    return new Metrics.Context("group_by.fetch", null, groupByName, null, false, null, null, null, null);
  }

  public CompletableFuture<List<JavaStatsResponse>> fetchStats(JavaStatsRequest request) {
    Future<Seq<Fetcher.StatsResponse>> responses = this.fetcher.fetchStats(request.toScalaRequest());
    // Convert responses to CompletableFuture
    return convertStatsResponses(responses);
  }

  public CompletableFuture<JavaMergedStatsResponse> fetchMergedStatsBetween(JavaStatsRequest request) {
    Future<Fetcher.MergedStatsResponse> response = this.fetcher.fetchMergedStatsBetween(request.toScalaRequest());
    // Convert responses to CompletableFuture
    return FutureConverters.toJava(response).toCompletableFuture().thenApply(JavaFetcher::toJavaMergedStatsResponse);
  }

  public CompletableFuture<JavaSeriesStatsResponse> fetchStatsTimeseries(JavaStatsRequest request) {
    Future<Fetcher.SeriesStatsResponse> response = this.fetcher.fetchStatsTimeseries(request.toScalaRequest());
    // Convert responses to CompletableFuture
    return FutureConverters.toJava(response).toCompletableFuture().thenApply(JavaFetcher::toJavaSeriesStatsResponse);
  }

  public Map<String, DataType> retrieveJoinSchema(String joinName) {
    return JavaConverters.mapAsJavaMap(this.fetcher.retrieveJoinSchema(joinName));
  }

  public Map<String, DataType> retrieveGroupBySchema(String groupByName) {
    return JavaConverters.mapAsJavaMap(this.fetcher.retrieveGroupBySchema(groupByName));
  }

  public Map<String, DataType> retrieveJoinKeys(String joinName) {
    return JavaConverters.mapAsJavaMap(this.fetcher.retrieveJoinKeys(joinName));
  }

  public Map<String, DataType> retrieveGroupByKeys(String groupByName) {
    return JavaConverters.mapAsJavaMap(this.fetcher.retrieveGroupByKeys(groupByName));
  }

  /**
   * Set the partition meta format for the fetcher.
   * @param format - The date format to use for parsing. (i.e. "yyyy-MM-dd")
   */
  public void setPartitionMeta(String format) {
    this.fetcher.setPartitionMeta(format);
  }

  /**
   * Allow users to set the join configuration for the fetcher.
   */
  public void putJoinConf(String joinName, String joinConfigString) throws UnsupportedEncodingException {
    KVStore.PutRequest joinPutReq = new KVStore.PutRequest(
            ("joins/" + joinName).getBytes(Constants.UTF8()),
            joinConfigString.getBytes(Constants.UTF8()),
            fetcher.dataset(),
            Option.empty());

    this.fetcher.kvStore().put(joinPutReq);
  }
}

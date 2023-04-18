package ai.chronon.online;

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
    long startTs = System.currentTimeMillis();
    Future<Seq<Response>> responses = this.fetcher.fetchJoin(convertJavaRequestList(requests));
    // Convert responses to CompletableFuture
    CompletableFuture<List<JavaResponse>> jRespFuture = convertResponses(responses);
    jRespFuture.thenApply(jResps -> {
      for (JavaResponse jResp : jResps) {
        Metrics.Context ctx = getJoinContext(jResp.request.name);
        ctx.histogram("java.overall.latency.millis", System.currentTimeMillis() - startTs);
      }
      return jResps;
    });
    return jRespFuture;
  }

  private Metrics.Context getJoinContext(String joinName) {
    return new Metrics.Context(
        "join.fetch",
        joinName,
        null,
        null,
        false,
        null,
        null,
        null,
        null
    );
  }

}

package ai.chronon.online;

import scala.collection.Seq;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// helper method to writeExternalSourceHandlers in java/kt etc.
// the override method takes care of mapping to and from
//  scala.util.Try  -> ai.chronon.online.JTry
//  scala.collection.immutable.Map  -> java.util.Map
//  scala.collection.immutable.List -> java.util.List
//  scala.concurrent.Future         -> java.util.concurrent.CompletableFuture
public abstract class JavaExternalSourceHandler extends ExternalSourceHandler {

    //java friendly method
    public abstract CompletableFuture<java.util.List<JavaResponse>> fetchJava(java.util.List<JavaRequest> requests);

    @Override
    public Future<Seq<Fetcher.Response>> fetch(Seq<Fetcher.Request> requests) {
        java.util.List<JavaRequest> javaRequests = ScalaVersionSpecificCollectionsConverter
                .convertScalaListToJava(requests.toList())
                .stream()
                .map(JavaRequest::fromScalaRequest)
                .collect(Collectors.toList());

        CompletableFuture<java.util.List<JavaResponse>> jResultFuture = fetchJava(javaRequests);
        CompletableFuture<Seq<Fetcher.Response>> mapJFuture = jResultFuture.thenApply(jList -> {
                    java.util.List<Fetcher.Response> jListSMap = jList
                            .stream()
                            .map(JavaResponse::toScala)
                            .collect(Collectors.toList());
                    return ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(jListSMap).toSeq();
                }
        );
        return FutureConverters.toScala(mapJFuture);
    }
}

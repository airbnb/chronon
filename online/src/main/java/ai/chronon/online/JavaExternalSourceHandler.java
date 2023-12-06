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
        // TODO: deprecate ScalaVersionSpecificCollectionsConverter in java
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

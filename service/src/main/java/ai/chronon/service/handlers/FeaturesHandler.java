package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.service.model.GetFeaturesResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static ai.chronon.online.JTry.failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;

public class FeaturesHandler implements Handler<RoutingContext> {

    public enum EntityType {
        GroupBy,
        Join
    }

    private static final Logger logger = LoggerFactory.getLogger(FeaturesHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final EntityType entityType;
    private final JavaFetcher fetcher;

    public FeaturesHandler(EntityType entityType, JavaFetcher fetcher) {
        this.entityType = entityType;
        this.fetcher = fetcher;
    }

    @Override
    public void handle(RoutingContext ctx) {
        String entityName = ctx.pathParam("name");
        logger.info("Retrieving {} - {}", entityType.name(), entityName);
        JTry<List<JavaRequest>> maybeRequest = parseJavaRequest(entityName, ctx.body());
        if (! maybeRequest.isSuccess()) {
            logger.error("Unable to parse request body", maybeRequest.getException());
            List<String> errorMessages = List.of(maybeRequest.getException().getMessage());
            ctx.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", errorMessages).encode());
            return;
        }

        List<JavaRequest> requests = maybeRequest.getValue();
        CompletableFuture<List<JavaResponse>> resultsJavaFuture =
                entityType.equals(EntityType.GroupBy) ? fetcher.fetchGroupBys(requests) : fetcher.fetchJoin(requests);
        Future<List<JTry<Map<String, Object>>>> maybeFeatureResponses =
                Future.fromCompletionStage(resultsJavaFuture)
                      .map(result -> result.stream().map(FeaturesHandler::responseToMap)
                      .collect(Collectors.toList()));

        maybeFeatureResponses.onSuccess(resultList -> {
            // as this is a bulkGet request, we might have some successful and some failed responses
            // we return the responses in the same order as they come in and mark them as successful / failed based
            // on the lookups
            GetFeaturesResponse.Builder responseBuilder = GetFeaturesResponse.builder();
            List<GetFeaturesResponse.Result> results = resultList.stream().map(tryObj -> {
                if (tryObj.isSuccess()) {
                    return GetFeaturesResponse.Result.builder().status(Success).features(tryObj.getValue()).build();
                } else {
                    return GetFeaturesResponse.Result.builder().status(Failure).error(tryObj.getException().getMessage()).build();
                }
            }).collect(Collectors.toList());
            responseBuilder.results(results);

            ctx.response()
                        .setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(JsonObject.mapFrom(responseBuilder.build()).encode());
        });

        maybeFeatureResponses.onFailure(err -> {
            List<String> failureMessages = List.of(err.getMessage());
            ctx.response()
                    .setStatusCode(500)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", failureMessages).encode());
        });
    }

    public static JTry<Map<String, Object>> responseToMap(JavaResponse response) {
        return response.values;
    }

    public static JTry<List<JavaRequest>> parseJavaRequest(String name, RequestBody body) {
        TypeReference<List<Map<String, Object>>> ref = new TypeReference<>() { };
        try {
            List<Map<String, Object>> entityKeysList = objectMapper.readValue(body.asString(), ref);
            List<JavaRequest> requests = entityKeysList.stream().map(m -> new JavaRequest(name, m)).collect(Collectors.toList());
            return JTry.success(requests);
        } catch (Exception e) {
            return failure(e);
        }
    }
}

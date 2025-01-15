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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;

/**
 * Concrete implementation of the GetFeatures endpoints. Supports loading groupBys and joins.
 * Some notes on this:
 * We currently support bulkGet lookups against a single groupBy / join. Attempts to lookup n different GroupBys / Joins
 * need to be split up into n different requests.
 * A given bulkGet request might result in some successful lookups and some failed ones. We return a 4xx or 5xx response
 * if the overall request fails (e.g. we're not able to parse the input json, Future failure due to Api returning an error)
 * Individual failure responses will be marked as 'Failed' however the overall response status code will be successful (200)
 * The response list maintains the same order as the incoming request list.
 * As an example:
 * { results: [ {"status": "Success", "features": ...}, {"status": "Failure", "error": ...} ] }
 */
public class FeaturesHandler implements Handler<RoutingContext> {

    public enum EntityType {
        GroupBy,
        Join
    }

    // PoJo to simplify transforming responses from the Fetcher to the final results form we return
    private static class EntityKeyToValues {
        public EntityKeyToValues(Map<String, Object> keys, JTry<Map<String, Object>> values) {
            this.entityKeys = keys;
            this.features = values;
        }

        public Map<String, Object> entityKeys;
        public JTry<Map<String, Object>> features;
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
        logger.debug("Retrieving {} - {}", entityType.name(), entityName);
        JTry<List<JavaRequest>> maybeRequest = parseJavaRequest(entityName, ctx.body());
        if (! maybeRequest.isSuccess()) {
            logger.error("Unable to parse request body", maybeRequest.getException());
            List<String> errorMessages = Collections.singletonList(maybeRequest.getException().getMessage());
            ctx.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", errorMessages).encode());
            return;
        }

        List<JavaRequest> requests = maybeRequest.getValue();
        CompletableFuture<List<JavaResponse>> resultsJavaFuture =
                entityType.equals(EntityType.GroupBy) ? fetcher.fetchGroupBys(requests) : fetcher.fetchJoin(requests);
        // wrap the Java future we get in a Vert.x Future to not block the worker thread
        Future<List<EntityKeyToValues>> maybeFeatureResponses =
                Future.fromCompletionStage(resultsJavaFuture)
                      .map(result -> result.stream().map(FeaturesHandler::responseToPoJo)
                      .collect(Collectors.toList()));

        maybeFeatureResponses.onSuccess(resultList -> {
            // as this is a bulkGet request, we might have some successful and some failed responses
            // we return the responses in the same order as they come in and mark them as successful / failed based
            // on the lookups
            GetFeaturesResponse.Builder responseBuilder = GetFeaturesResponse.builder();
            List<GetFeaturesResponse.Result> results = resultList.stream().map(resultsPojo -> {
                if (resultsPojo.features.isSuccess()) {
                    return GetFeaturesResponse.Result.builder().status(Success).entityKeys(resultsPojo.entityKeys).features(resultsPojo.features.getValue()).build();
                } else {
                    return GetFeaturesResponse.Result.builder().status(Failure).entityKeys(resultsPojo.entityKeys).error(resultsPojo.features.getException().getMessage()).build();
                }
            }).collect(Collectors.toList());
            responseBuilder.results(results);

            ctx.response()
                        .setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(JsonObject.mapFrom(responseBuilder.build()).encode());
        });

        maybeFeatureResponses.onFailure(err -> {
            List<String> failureMessages = Collections.singletonList(err.getMessage());
            ctx.response()
                    .setStatusCode(500)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", failureMessages).encode());
        });
    }

    public static EntityKeyToValues responseToPoJo(JavaResponse response) {
        return new EntityKeyToValues(response.request.keys, response.values);
    }

    public static JTry<List<JavaRequest>> parseJavaRequest(String name, RequestBody body) {
        TypeReference<List<Map<String, Object>>> ref = new TypeReference<List<Map<String, Object>>>() { };
        try {
            List<Map<String, Object>> entityKeysList = objectMapper.readValue(body.asString(), ref);
            List<JavaRequest> requests = entityKeysList.stream().map(m -> new JavaRequest(name, m)).collect(Collectors.toList());
            return JTry.success(requests);
        } catch (Exception e) {
            return JTry.failure(e);
        }
    }
}

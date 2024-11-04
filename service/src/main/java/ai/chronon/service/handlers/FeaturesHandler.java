package ai.chronon.service.handlers;

import ai.chronon.online.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static ai.chronon.online.JTry.failure;

public class FeaturesHandler {
    private static final Logger logger = LoggerFactory.getLogger(FeaturesHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Router createFeaturesRoutes(Vertx vertx, Api api) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        JavaFetcher fetcher = api.buildJavaFetcher("feature-service", false);

        router.post("/groupby/:groupByName").handler(ctx -> {
            String groupByName = ctx.pathParam("groupByName");
            logger.info("Retrieving groupBy - {}", groupByName);
            JTry<JavaRequest> maybeRequest = parseJavaRequest(groupByName, ctx.body());
            if (! maybeRequest.isSuccess()) {
                logger.error("Unable to parse request body", maybeRequest.getException());
                ctx.fail(400, maybeRequest.getException());
            } else {
                List<JavaRequest> requests = List.of(maybeRequest.getValue());
                CompletableFuture<List<JavaResponse>> resultsJavaFuture = fetcher.fetchGroupBys(requests);
                Future<List<JTry<String>>> map = Future.fromCompletionStage(resultsJavaFuture)
                        .map(result -> result.stream().map(FeaturesHandler::responseToJson).collect(Collectors.toList()));
                map.onSuccess(result -> {
                    // collect any failures
                   List<String> failureMessages = result.stream().filter(jt -> !jt.isSuccess()).map(jt -> jt.getException().getMessage()).collect(Collectors.toList());
                   // fail the request if there are any failures
                   if (! failureMessages.isEmpty()) {
                       ctx.response()
                               .setStatusCode(500)
                               .putHeader("content-type", "application/json")
                               .end(new JsonObject().put("errors", failureMessages).encode());
                   } else {
                       List<String> featureResults = result.stream().map(JTry::getValue).collect(Collectors.toList());
                       ctx.response()
                               .setStatusCode(200)
                               .putHeader("content-type", "application/json")
                               .end(new JsonObject().put("features", featureResults).encode());
                   }
                });
                map.onFailure(err -> {
                    List<String> failureMessages = List.of(err.getMessage());
                    ctx.response()
                            .setStatusCode(500)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("errors", failureMessages).encode());
                });
            }
        });

        router.post("/join/:joinName").handler(ctx -> {
            String joinName = ctx.pathParam("joinName");
            logger.info("Retrieving join - {}", joinName);
            ctx.response()
                    .putHeader("content-type", "application/json")
                    .end("{\"status\": \"created\"}");
        });

        return router;
    }

    public static JTry<String> responseToJson(JavaResponse response) {
        if(response.values.isSuccess()) {
            Map<String, Object> featureMap = response.values.getValue();
            try {
                return JTry.success(objectMapper.writeValueAsString(featureMap));
            } catch (JsonProcessingException e) {
                return JTry.failure(e);
            }
        } else {
            return JTry.failure(response.values.getException());
        }
    }

    public static JTry<JavaRequest> parseJavaRequest(String name, RequestBody body) {
        TypeReference<Map<String, Object>> ref = new TypeReference<>() { };
        try {
            Map<String, Object> entityKeysMap = objectMapper.readValue(body.asString(), ref);
            return JTry.success(new JavaRequest(name, entityKeysMap));
        } catch (Exception e) {
            return failure(e);
        }
    }
}

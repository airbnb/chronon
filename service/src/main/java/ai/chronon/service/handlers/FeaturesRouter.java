package ai.chronon.service.handlers;

import ai.chronon.online.*;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import static ai.chronon.service.handlers.FeaturesHandler.EntityType.GroupBy;
import static ai.chronon.service.handlers.FeaturesHandler.EntityType.Join;

// Configures the routes for our get features endpoints
// We support bulkGets of groupBys and bulkGets of joins
public class FeaturesRouter {

    public static Router createFeaturesRoutes(Vertx vertx, Api api) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        JavaFetcher fetcher = api.buildJavaFetcher("feature-service", false);

        router.post("/groupby/:name").handler(new FeaturesHandler(GroupBy, fetcher));
        router.post("/join/:name").handler(new FeaturesHandler(Join, fetcher));

        return router;
    }
}

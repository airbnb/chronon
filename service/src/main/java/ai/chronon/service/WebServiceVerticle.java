package ai.chronon.service;

import ai.chronon.online.Api;
import ai.chronon.service.handlers.FeaturesRouter;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Chronon webservice. We wire up our API routes and configure and launch our HTTP service here.
 * We choose to use just 1 verticle for now as it allows us to keep things simple and we don't need to scale /
 * independently deploy different endpoint routes.
 */
public class WebServiceVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(WebServiceVerticle.class);

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        ConfigStore cfgStore = new ConfigStore(vertx);
        startHttpServer(cfgStore.getServerPort(), cfgStore.encodeConfig(), ApiProvider.buildApi(cfgStore), startPromise);
    }

    protected void startHttpServer(int port, String configJsonString, Api api, Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);

        // Define routes

        // Set up sub-routes for the various feature retrieval apis
        router.route("/v1/features/*").subRouter(FeaturesRouter.createFeaturesRoutes(vertx, api));

        // Health check route
        router.get("/ping").handler(ctx -> {
            ctx.json("Pong!");
        });

        // Add route to show current configuration
        router.get("/config").handler(ctx -> {
            ctx.response()
               .putHeader("content-type", "application/json")
               .end(configJsonString);
        });

        // Start HTTP server
        HttpServerOptions httpOptions =
                new HttpServerOptions()
                        .setTcpKeepAlive(true)
                        .setIdleTimeout(60);
        server = vertx.createHttpServer(httpOptions);
        server.requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    logger.info("HTTP server started on port {}", server.actualPort());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to start HTTP server", err);
                    startPromise.fail(err);
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        logger.info("Stopping HTTP server...");
        if (server != null) {
            server.close()
                    .onSuccess(v -> {
                        logger.info("HTTP server stopped successfully");
                        stopPromise.complete();
                    })
                    .onFailure(err -> {
                        logger.error("Failed to stop HTTP server", err);
                        stopPromise.fail(err);
                    });
        } else {
            stopPromise.complete();
        }
    }
}

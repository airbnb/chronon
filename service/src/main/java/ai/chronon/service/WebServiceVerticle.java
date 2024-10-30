package ai.chronon.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Chronon webservice. We choose to use just 1 verticle for now as it allows us to
 * keep things simple and we don't need to scale / independently deploy different endpoint routes.
 * To run:
 * $ sbt "project service" clean assembly
 * $ java -jar service/target/scala-2.12/service-vertx_service-0.0.86-SNAPSHOT.jar run ai.chronon.service.WebServiceVerticle -Dserver.port=9000 -Donline.jar=/Users/piyush/workspace/airbnb-chronon/quickstart/mongo-online-impl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar -Donline.class=ai.chronon.quickstart.online.ChrononMongoOnlineImpl
 */
public class WebServiceVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(WebServiceVerticle.class);

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        ConfigStore cfgStore = new ConfigStore(vertx);
        startHttpServer(cfgStore.getServerPort(), cfgStore.encodeConfig(), startPromise);
    }

    protected void startHttpServer(int port, String configJsonString, Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);

        // Define routes
        router.get("/ping").handler(ctx -> {
            ctx.json("Pong!");
        });

        // Add route to show current configuration
        router.get("/config").handler(ctx -> {
            ctx.response()
               .putHeader("content-type", "application/json")
               .end(configJsonString);
        });

        //Api myApi = loadApiImplementation();

        // Start HTTP server
        server = vertx.createHttpServer();
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

//    private Api loadApiImplementation() throws Exception {
//        String jarPath = System.getProperty("online.jar");
//        String className = System.getProperty("online.class");
//
//        if (jarPath == null || className == null) {
//            throw new IllegalArgumentException(
//                    "Both 'online.jar' and 'online.class' system properties must be set. " +
//                            "Current values - jar: " + jarPath + ", class: " + className
//            );
//        }
//
//        File jarFile = new File(jarPath);
//        if (!jarFile.exists()) {
//            throw new IllegalArgumentException("JAR file does not exist: " + jarPath);
//        }
//
//        logger.info("Loading API implementation from JAR: {}, class: {}", jarPath, className);
//
//        // Create class loader for the API JAR
//        URL jarUrl = jarFile.toURI().toURL();
//        URLClassLoader apiClassLoader = new URLClassLoader(
//                new URL[]{jarUrl},
//                this.getClass().getClassLoader()
//        );
//
//        // Load and instantiate the API implementation
//        Class<?> apiClass = Class.forName(className, true, apiClassLoader);
//        if (!Api.class.isAssignableFrom(apiClass)) {
//            throw new IllegalArgumentException(
//                    "Class " + className + " does not extend the Api abstract class"
//            );
//        }
//
//        Map<String, String> myMap = Map.of();
//        scala.collection.immutable.Map<String, String> scalaPropsMap = ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(myMap);
//
//        Api apiInstance = (Api) apiClass.getConstructors()[0].newInstance(scalaPropsMap);
//        logger.info("Successfully loaded API implementation: {}", className);
//        return apiInstance;
//    }
}


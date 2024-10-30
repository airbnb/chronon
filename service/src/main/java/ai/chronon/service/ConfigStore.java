package ai.chronon.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Helps keep track of the various Chronon fetcher service configs.
 * We currently read configs once at startup - this makes sense for configs
 * such as the server port and we can revisit / extend things in the future to
 * be able to hot-refresh configs like Vertx supports under the hood.
 */
public class ConfigStore {

    private static final int DEFAULT_PORT = 8080;
    private static final String SERVER_PORT = "server.port";

    private JsonObject jsonConfig;

    public ConfigStore(Vertx vertx) {
        ConfigRetriever configRetriever = ConfigRetriever.create(vertx);
        configRetriever.getConfig().onComplete(ar -> {
            if (ar.failed()) {
                throw new IllegalStateException("Unable to load service config", ar.cause());
            }
            jsonConfig = ar.result();
        });
    }

    public int getServerPort() {
        return jsonConfig.getInteger(SERVER_PORT, DEFAULT_PORT);
    }

    public String encodeConfig() {
        return jsonConfig.encodePrettily();
    }
}

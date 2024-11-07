package ai.chronon.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Helps keep track of the various Chronon fetcher service configs.
 * We currently read configs once at startup - this makes sense for configs
 * such as the server port and we can revisit / extend things in the future to
 * be able to hot-refresh configs like Vertx supports under the hood.
 */
public class ConfigStore {

    private static final int DEFAULT_PORT = 8080;

    private static final String SERVER_PORT = "server.port";
    private static final String ONLINE_JAR = "online.jar";
    private static final String ONLINE_CLASS = "online.class";
    private static final String ONLINE_API_PROPS = "online.api.props";

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

    public Optional<String> getOnlineJar() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_JAR));
    }

    public Optional<String> getOnlineClass() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_CLASS));
    }

    public Map<String, String> getOnlineApiProps() {
        JsonObject apiProps = jsonConfig.getJsonObject(ONLINE_API_PROPS);
        if (apiProps == null) {
            return new HashMap<String, String>();
        }

        return apiProps.stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> String.valueOf(e.getValue())
        ));
    }

    public String encodeConfig() {
        return jsonConfig.encodePrettily();
    }
}

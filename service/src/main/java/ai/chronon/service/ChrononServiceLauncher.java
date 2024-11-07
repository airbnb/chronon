package ai.chronon.service;

import ai.chronon.online.Metrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom launcher to help configure the Chronon vertx feature service
 * to handle things like setting up a statsd metrics registry.
 * We use statsd here to be consistent with the rest of our project (e.g. fetcher code).
 * This allows us to send Vertx webservice metrics along with fetcher related metrics to allow users
 * to debug performance issues and set alerts etc.
 */
public class ChrononServiceLauncher extends Launcher {

    @Override
    public void beforeStartingVertx(VertxOptions options) {

        StatsdConfig config = new StatsdConfig() {
            private final String statsdHost = Metrics.Context$.MODULE$.statsHost();
            private final String statsdPort = String.valueOf(Metrics.Context$.MODULE$.statsPort());

            final Map<String, String> statsProps = new HashMap<String, String>() {{
                put(prefix() + "." + "port", statsdPort);
                put(prefix() + "." + "host", statsdHost);
                put(prefix() + "." + "protocol", Integer.parseInt(statsdPort) == 0 ? "UDS_DATAGRAM" : "UDP");
            }};

            @Override
            public String get(String key) {
                return statsProps.get(key);
            }
        };

        MeterRegistry registry = new StatsdMeterRegistry(config, Clock.SYSTEM);
        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory(registry);

        // Configure metrics via statsd
        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    public static void main(String[] args) {
        new ChrononServiceLauncher().dispatch(args);
    }
}

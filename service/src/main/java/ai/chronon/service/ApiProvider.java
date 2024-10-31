package ai.chronon.service;

import ai.chronon.online.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Optional;

public class ApiProvider {
    public final Api api;
    private static final Logger logger = LoggerFactory.getLogger(ApiProvider.class);

    public ApiProvider(ConfigStore configStore) throws Exception {
        Optional<String> maybeJarPath = configStore.getOnlineJar();
        Optional<String> maybeClass = configStore.getOnlineClass();
        if (maybeJarPath.isEmpty() || maybeClass.isEmpty()) {
            throw new IllegalArgumentException("Both 'online.jar' and 'online.class' configs must be set.");
        }

        String jarPath = maybeJarPath.get();
        String className = maybeClass.get();
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("JAR file does not exist: " + jarPath);
        }

        logger.info("Loading API implementation from JAR: {}, class: {}", jarPath, className);

        // Create class loader for the API JAR
        URL jarUrl = jarFile.toURI().toURL();
        URLClassLoader apiClassLoader = new URLClassLoader(
                new URL[]{jarUrl},
                this.getClass().getClassLoader()
        );

        // Load and instantiate the API implementation
        Class<?> apiClass = Class.forName(className, true, apiClassLoader);
        if (!Api.class.isAssignableFrom(apiClass)) {
            throw new IllegalArgumentException(
                    "Class " + className + " does not extend the Api abstract class"
            );
        }

        Map<String, String> propsMap = configStore.getOnlineApiProps();
        scala.collection.immutable.Map<String, String> scalaPropsMap = ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(propsMap);

        this.api = (Api) apiClass.getConstructors()[0].newInstance(scalaPropsMap);
    }
}

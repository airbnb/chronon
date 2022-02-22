package ai.zipline.online;

import scala.collection.JavaConverters;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class JavaResponse {
    public JavaRequest request;
    public Map<String, Object> values;

    public JavaResponse(Fetcher.Response scalaResponse){
        this.request = new JavaRequest(scalaResponse.request());
        if (scalaResponse.values().isFailure()) {
            values = new HashMap<>();
            Throwable t = scalaResponse.values().failed().get();
            values.put(request.name + "_exception", t);
        } else if (scalaResponse.values().get() == null || scalaResponse.values().get().isEmpty()) {
            this.values = Collections.emptyMap();
        } else {
            this.values = JavaConverters.mapAsJavaMapConverter(scalaResponse.values().get()).asJava();
        }
    }
}

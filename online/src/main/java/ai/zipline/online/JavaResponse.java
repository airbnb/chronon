package ai.zipline.online;

import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.Map;


public class JavaResponse {
    public JavaRequest request;
    public Map<String, Object> values;

    public JavaResponse(Fetcher.Response scalaResponse) {
        this.request = new JavaRequest(scalaResponse.request());
        if (scalaResponse.values() == null || scalaResponse.values().isEmpty()) {
            this.values = Collections.emptyMap();
        } else {
            this.values = JavaConverters.mapAsJavaMapConverter(scalaResponse.values()).asJava();
        }
    }
}

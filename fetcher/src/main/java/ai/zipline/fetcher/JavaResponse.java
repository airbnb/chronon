package ai.zipline.fetcher;

import java.util.Map;
import scala.collection.JavaConverters;;

public class JavaResponse {
  public JavaRequest request;
  public Map<String, Object> values;

  public JavaResponse(Fetcher.Response scalaResponse) {
    this.request = new JavaRequest(scalaResponse.request());
    this.values = JavaConverters.mapAsJavaMapConverter(scalaResponse.values()).asJava();
  }
}

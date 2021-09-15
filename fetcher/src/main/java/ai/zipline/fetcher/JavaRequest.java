package ai.zipline.fetcher;

import java.util.Map;
import scala.Option;
import scala.Predef;
import scala.collection.JavaConverters;

public class JavaRequest {
  public String name;
  public Map<String, Object> keys;
  public Long atMillis;

  public JavaRequest(String name, Map<String, Object> keys) {
    this(name, keys, null);
  }

  public JavaRequest(String name, Map<String, Object> keys, Long atMillis) {
    this.name = name;
    this.keys = keys;
    this.atMillis = atMillis;
  }

  public JavaRequest(Fetcher.Request scalaRequest) {
    this.name = scalaRequest.name();
    this.keys = JavaConverters.mapAsJavaMapConverter(scalaRequest.keys()).asJava();
    this.atMillis = scalaRequest.atMillis().getOrElse(null);
  }

  public Fetcher.Request toScalaRequest() {
    return new Fetcher.Request(
        this.name,
        JavaConverters.mapAsScalaMapConverter(keys)
            .asScala()
            .toMap(Predef.conforms()),
        Option.apply(this.atMillis));
  }
}

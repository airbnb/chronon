package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;


public class JavaStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> values;
    public Long millis;

    public JavaStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
        this.millis = null;
    }

    public JavaStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values, Long millis) {
        this.request = request;
        this.values = values;
        this.millis = millis;
    }

    public JavaStatsResponse(Fetcher.StatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
        this.millis = scalaResponse.millis();
    }

}

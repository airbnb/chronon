package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;


public class JavaStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> values;

    public JavaStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
    }

    public JavaStatsResponse(Fetcher.StatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
    }

    public Fetcher.StatsResponse toScala() {
        return new Fetcher.StatsResponse(
                request.toScalaRequest(),
                values.map(ScalaVersionSpecificCollectionsConverter::convertJavaMapToScala).toScala());
    }
}

package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;

public class JavaMergedStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> values;

    public JavaMergedStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
    }

    public JavaMergedStatsResponse(Fetcher.MergedStatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
    }

    public Fetcher.MergedStatsResponse toScala() {
        return new Fetcher.MergedStatsResponse(
                request.toScalaRequest(),
                values.map(ScalaVersionSpecificCollectionsConverter::convertJavaMapToScala).toScala());
    }
}

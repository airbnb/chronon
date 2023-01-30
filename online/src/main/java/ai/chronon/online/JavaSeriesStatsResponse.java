package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;

public class JavaSeriesStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> values;

    public JavaSeriesStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> series) {
        this.request = request;
        this.values = series;
    }

    public JavaSeriesStatsResponse(Fetcher.SeriesStatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
    }

    public Fetcher.SeriesStatsResponse toScala() {
        return new Fetcher.SeriesStatsResponse(
                request.toScalaRequest(),
                values.map(ScalaVersionSpecificCollectionsConverter::convertJavaMapToScala).toScala());
    }
}

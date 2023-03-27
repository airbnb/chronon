package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;

public class JavaSeriesStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> series;

    public JavaSeriesStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> series) {
        this.request = request;
        this.series = series;
    }

    public JavaSeriesStatsResponse(Fetcher.SeriesStatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.series = JTry
                .fromScala(scalaResponse.series())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
    }

    public Fetcher.SeriesStatsResponse toScala() {
        return new Fetcher.SeriesStatsResponse(
                request.toScalaRequest(),
                series.map(ScalaVersionSpecificCollectionsConverter::convertJavaMapToScala).toScala());
    }
}

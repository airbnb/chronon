package ai.chronon.online;

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;


public class JavaResponse {
    public JavaRequest request;
    public JTry<Map<String, Object>> values;

    public JavaResponse(JavaRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
    }

    public JavaResponse(Fetcher.Response scalaResponse){
        this.request = new JavaRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
    }

    public Fetcher.Response toScala(){
        return new Fetcher.Response(
                request.toScalaRequest(),
                values.map(ScalaVersionSpecificCollectionsConverter::convertJavaMapToScala).toScala());
    }
}

package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import io.vertx.ext.web.RequestBody;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FeaturesHandlerTest {

    @Test
    public void testParsingOfSimpleJavaRequests() {
        String mockRequest = "{\"user\":\"user1\",\"zip\":10010}";
        RequestBody mockRequestBody = mock(RequestBody.class);
        when(mockRequestBody.asString()).thenReturn(mockRequest);

        String groupByName = "my_groupby.1";
        JTry<JavaRequest> maybeRequest = FeaturesHandler.parseJavaRequest(groupByName, mockRequestBody);
        assertTrue(maybeRequest.isSuccess());
        JavaRequest req = maybeRequest.getValue();
        assertEquals(req.name, groupByName);
        assertTrue(req.keys.containsKey("user") && req.keys.get("user").getClass().equals(String.class));
        assertTrue(req.keys.containsKey("zip") && req.keys.get("zip").getClass().equals(Integer.class));
    }

    @Test
    public void testParsingInvalidRequest() {
        // mess up the colon after the zip field
        String mockRequest = "{\"user\":\"user1\",\"zip\"10010}";
        RequestBody mockRequestBody = mock(RequestBody.class);
        when(mockRequestBody.asString()).thenReturn(mockRequest);

        String groupByName = "my_groupby.1";
        JTry<JavaRequest> maybeRequest = FeaturesHandler.parseJavaRequest(groupByName, mockRequestBody);
        assertFalse(maybeRequest.isSuccess());
        assertNotNull(maybeRequest.getException());
    }

    @Test
    public void testResponseToJson() throws Exception {
        JavaRequest myReq = new JavaRequest("my_groupby.1", Map.of("key", "value"));
        Map<String, Object> mockFeatureValues = Map.of("feature_1", 12, "feature_2", 34.4);
        JavaResponse happyResponse = new JavaResponse(myReq, JTry.success(mockFeatureValues));

        JTry<String> jsonTry = FeaturesHandler.responseToJson(happyResponse);
        assertTrue(jsonTry.isSuccess());

        // test round trip to confirm values what the 'service' sent
        String json = jsonTry.getValue();
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<Map<String, Object>> ref = new TypeReference<>() { };
        Map<String, Object> parsedFeatureValues = objectMapper.readValue(json, ref);

        assertEquals(mockFeatureValues, parsedFeatureValues);
    }

    @Test
    public void testFailureResponseToJson() throws Exception {
        JavaRequest myReq = new JavaRequest("my_groupby.1", Map.of("key", "value"));
        JavaResponse failureResponse = new JavaResponse(myReq, JTry.failure(new IllegalArgumentException("Something broke in Chronon")));

        JTry<String> jsonTry = FeaturesHandler.responseToJson(failureResponse);
        assertFalse(jsonTry.isSuccess());
        assertNotNull(jsonTry.getException());
    }
}

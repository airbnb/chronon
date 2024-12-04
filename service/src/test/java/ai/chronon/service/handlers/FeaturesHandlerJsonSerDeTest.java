package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaRequest;
import io.vertx.ext.web.RequestBody;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FeaturesHandlerJsonSerDeTest {

    @Test
    public void testParsingOfSimpleJavaRequests() {
        String mockRequest = "[{\"user\":\"user1\",\"zip\":10010}]";
        RequestBody mockRequestBody = mock(RequestBody.class);
        when(mockRequestBody.asString()).thenReturn(mockRequest);

        String groupByName = "my_groupby.1";
        JTry<List<JavaRequest>> maybeRequest = FeaturesHandler.parseJavaRequest(groupByName, mockRequestBody);
        assertTrue(maybeRequest.isSuccess());
        List<JavaRequest> reqs = maybeRequest.getValue();
        assertEquals(1, reqs.size());
        JavaRequest req = reqs.get(0);
        assertEquals(req.name, groupByName);
        assertTrue(req.keys.containsKey("user") && req.keys.get("user").getClass().equals(String.class));
        assertTrue(req.keys.containsKey("zip") && req.keys.get("zip").getClass().equals(Integer.class));
    }

    @Test
    public void testParsingInvalidRequest() {
        // mess up the colon after the zip field
        String mockRequest = "[{\"user\":\"user1\",\"zip\"10010}]";
        RequestBody mockRequestBody = mock(RequestBody.class);
        when(mockRequestBody.asString()).thenReturn(mockRequest);

        String groupByName = "my_groupby.1";
        JTry<List<JavaRequest>> maybeRequest = FeaturesHandler.parseJavaRequest(groupByName, mockRequestBody);
        assertFalse(maybeRequest.isSuccess());
        assertNotNull(maybeRequest.getException());
    }

    @Test
    public void testParsingOneValidAndInvalidRequest() {
        String mockRequest = "[{\"user\":\"user1\",\"zip\":10010}, {\"user\":\"user1\",\"zip\"10010}]";
        RequestBody mockRequestBody = mock(RequestBody.class);
        when(mockRequestBody.asString()).thenReturn(mockRequest);

        String groupByName = "my_groupby.1";
        JTry<List<JavaRequest>> maybeRequest = FeaturesHandler.parseJavaRequest(groupByName, mockRequestBody);
        assertFalse(maybeRequest.isSuccess());
        assertNotNull(maybeRequest.getException());
    }
}

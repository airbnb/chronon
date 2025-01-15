package ai.chronon.service.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PoJo capturing the response we return back as part of /v1/features/groupby and /v1/features/join endpoints
 * when the individual bulkGet lookups were either all successful or partially successful.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetFeaturesResponse {
    private final List<Result> results;

    private GetFeaturesResponse(Builder builder) {
        this.results = builder.results;
    }

    public List<Result> getResults() {
        return results;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<Result> results = new ArrayList<>();

        public Builder results(List<Result> results) {
            this.results = results;
            return this;
        }

        public Builder addResult(Result result) {
            this.results.add(result);
            return this;
        }

        public GetFeaturesResponse build() {
            return new GetFeaturesResponse(this);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Result {
        public enum Status {
            Success,
            Failure
        }

        private final Status status;
        private final Map<String, Object> entityKeys;
        private final Map<String, Object> features;
        private final String error;

        private Result(Builder builder) {
            this.status = builder.status;
            this.entityKeys = builder.entityKeys;
            this.features = builder.features;
            this.error = builder.error;
        }

        public Status getStatus() {
            return status;
        }

        public Map<String, Object> getFeatures() {
            return features;
        }

        public Map<String, Object> getEntityKeys() {
            return entityKeys;
        }

        public String getError() {
            return error;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private Status status;
            private Map<String, Object> entityKeys;
            private Map<String, Object> features;
            private String error;

            public Builder status(Status status) {
                this.status = status;
                return this;
            }

            public Builder features(Map<String, Object> features) {
                this.features = features;
                return this;
            }

            public Builder entityKeys(Map<String, Object> entityKeys) {
                this.entityKeys = entityKeys;
                return this;
            }

            public Builder error(String error) {
                this.error = error;
                return this;
            }

            public Result build() {
                return new Result(this);
            }
        }
    }
}
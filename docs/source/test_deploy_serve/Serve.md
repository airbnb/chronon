# Serving Chronon Data

The main way to serve production Chronon data online is with the Java or Scala Fetcher Libraries. To use these integrations, you simply need to import the fetcher and embed it within your service or application.

The main online Java Fetcher libraries can be found here:

1. [`JavaFetcher`](https://github.com/airbnb/chronon/blob/main/online/src/main/java/ai/chronon/online/JavaFetcher.java)
2. [`JavaRequest`](https://github.com/airbnb/chronon/blob/main/online/src/main/java/ai/chronon/online/JavaRequest.java)
3. [`JavaResponse`](https://github.com/airbnb/chronon/blob/main/online/src/main/java/ai/chronon/online/JavaResponse.java)

And their scala counterparts:

1. [`Fetcher`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Fetcher.scala)
2. [`Request`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Fetcher.scala#L39)
3. [`Response`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Fetcher.scala#L48)

Example Implementation

```java
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import your.internal.implementation.of.Api as InternalAPIImpl;

// initialize the Chronon fetcher only once per process.
JavaFetcher ChrononJavaFetcher = InternalAPIImpl.buildJavaFetcher();

// In your request handler
ChrononJavaFetcher.fetchJoin(requests);
```

For details on how to write your internal implementation of the online API, see the [Online Integration](../setup/Online_Integration.md) documentation. This should only be implemented once for your organization, then online serving use cases can use the above API.

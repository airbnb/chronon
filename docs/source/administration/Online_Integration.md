# Online Integration

This document covers how to integrate Chronon with your online KV store, which is the backend that powers low latency serving of individual feature vectors in the online environment.

This integration gives Chronon the ability to:

1. Perform batch uploads of feature values to the KV store
2. Perform streaming updates to those features
3. Fetch features via the `Fetcher` API

## Example

If you'd to start with an example, please refer to the [MongoDB Implementation in the Quickstart Guide](../../../quickstart/mongo-online-impl/). This provides a complete working example of how to integrate Chronon with MongoDB. 

## Components

**KVStore**: The biggest part of the API implementation is the [KVStore](../../../online/src/main/scala/ai/chronon/online/Api.scala#L43).

There are three functions to implement as part of this integration:

1. `create`: which takes a string and creates a new database/dataset with that name.
2. `multiGet`: which takes a `Seq` of [`GetRequest`](../../../online/src/main/scala/ai/chronon/online/Api.scala#L33) and converts them into a `Future[Seq[GetResponse]]` by querying the underlying KVStore.
3. `multiPut`: which takes a `Seq` of [`PutRequest`](../../../online/src/main/scala/ai/chronon/online/Api.scala#L38) and converts them into `Future[Seq[Boolean]]` (success/fail) by attempting to insert them into the underlying KVStore.

See the [MongoDB example here](../../../quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/MongoKvStore.scala).

**StreamDecoder**: This is responsible for "decoding" or converting the raw values that Chronon streaming jobs will read into events that it knows how to process.

At a high level, there are two types of inputs streams that Chronon might listen to:

1. Events: These are the most common type of streaming data, and can be thought of as the "standard" insert-only kafka log. The key differentiator from Mutations is that Events are immutable, and cannot be updated/deleted (although new events with the same key can be emitted).
2. Mutations: These are streaming updates to specific entities. For example, an item in an online product catalog might get updates to its price or description, or it might get deleted altogether. Unlike normal events, these can be modeled as `INSERT/UPDATE/DELETE`.

In the API, the `Mutation` is modeled as the general case for both `Events` and `Mutations`, since `Events` can be viewed as a subset of `Mutation`.

The `StreamDecoder` is responsible for two function implementations:

1. `decode`: Which takes in an `Array[Byte]` and converts it to a `Mutation`,
2. `schema`: Which provides the `StructType` for the given `GroupBy`


See the [Quickstart example here](../../../quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/QuickstartMutationDecoder.scala).


**API:** The main API that requires implementation is [API](../../../online/src/main/scala/ai/chronon/online/Api.scala#L151). This combines the above implementations with other client and logging configuration.

[ChrononMongoOnlineImpl](../../../quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/ChrononMongoOnlineImpl.scala) Is an example implemenation of the API.

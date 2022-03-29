package ai.zipline.online;

import scala.Function1;
import scala.concurrent.Future;
import scala.runtime.AbstractFunction1;
import scala.util.Try;

import java.util.concurrent.CompletableFuture;

public class JConversions {
    @SuppressWarnings("unchecked")
    public static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> javaMap) {
        if (javaMap == null) return null;
        final java.util.List<scala.Tuple2<K, V>> list = new java.util.ArrayList<>(javaMap.size());
        for (final java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
        }
        final scala.collection.Seq<scala.Tuple2<K, V>> seq = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> toJavaCompletableFuture(scala.concurrent.Future<T> f, scala.concurrent.ExecutionContext ex) {
        ZCompletableFuture<T> cf = new ZCompletableFuture<T>(f);
        f.onComplete(new AbstractFunction1<Try<T>, CompletableFuture<T>>() {
            @Override
            public CompletableFuture<T> apply(Try<T> v1) {
                if (v1.isFailure()) {
                    cf.completeExceptionally(v1.failed().get());
                } else {
                    cf.complete(v1.get());
                }
                return cf;
            }
        }, ex);
        return cf;
    }
}

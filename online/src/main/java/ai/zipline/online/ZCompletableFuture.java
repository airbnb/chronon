package ai.zipline.online;

import scala.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class ZCompletableFuture<T> extends CompletableFuture<T> {

    public Future<T> wrapped;
    public ZCompletableFuture(Future<T> wrapped_) {
        this.wrapped = wrapped_;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T,? extends U> fn) { return thenApplyAsync(fn); }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return thenCombine(other, fn);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return acceptEitherAsync(other, action);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return applyToEitherAsync(other, fn);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return handleAsync(fn);
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return whenCompleteAsync(action);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return thenAcceptAsync(action);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return thenRunAsync(action);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return thenAcceptBothAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return runAfterBothAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action);
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        CompletableFuture<T> cf = new CompletableFuture<T>();
        whenCompleteAsync(new BiConsumer<T, Throwable>() {
            @Override
            public void accept(T t, Throwable throwable) {
                if(throwable == null) {
                    cf.complete(t);
                } else {
                    Object n = null;
                    try {
                        n = fn.apply(throwable);
                    } catch(Throwable th) {
                        cf.completeExceptionally(th);
                        n = this;
                    }
                    if(n != this) cf.complete((T) n);
                }
            }
        });
        return cf;
    }

    }

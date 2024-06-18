/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online;

import scala.util.Try;

import java.util.Objects;
import java.util.function.Function;

public abstract class JTry<V> {
    private JTry() {
    }

    public static <V> JTry<V> failure(Throwable t) {
        Objects.requireNonNull(t);
        return new Failure<>(t);
    }

    public static <V> JTry<V> success(V value) {
        return new Success<>(value);
    }

    public static <V> JTry<V> fromScala(Try<V> sTry) {
        if (sTry.isSuccess()) {
            return new Success<>(sTry.get());
        } else {
            return new Failure(sTry.failed().get());
        }
    }

    public abstract boolean isSuccess();

    public abstract Throwable getException();

    public abstract V getValue();

    public abstract <U> JTry<U> map(Function<? super V, ? extends U> f);

    public Try<V> toScala() {
        if (this.isSuccess()) {
            try {
                return new scala.util.Success<>(getValue());
            } catch (Throwable e) {
                throw new IllegalStateException("Invalid try with isSuccess=True " + this);
            }
        } else {
            return new scala.util.Failure(getException());
        }
    }

    private static class Failure<V> extends JTry<V> {

        private final Throwable exception;

        public Failure(Throwable t) {
            super();
            this.exception = t;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public Throwable getException() {
            return exception;
        }

        @Override
        public V getValue() {
            throw new RuntimeException(this.exception);
        }

        @Override
        public <U> JTry<U> map(Function<? super V, ? extends U> f) {
            Objects.requireNonNull(f);
            return JTry.failure(exception);
        }
    }

    private static class Success<V> extends JTry<V> {

        private final V value;

        public Success(V value) {
            super();
            this.value = value;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public Throwable getException() {
            throw new RuntimeException("Calling get exception on a successful object");
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public <U> JTry<U> map(Function<? super V, ? extends U> f) {
            Objects.requireNonNull(f);
            try {
                return JTry.success(f.apply(value));
            } catch (Throwable t) {
                return JTry.failure(t);
            }
        }
    }
}

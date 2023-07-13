package ai.chronon.online;

import java.io.Serializable;
import java.util.function.Function;

// spark closure cleaner needs a serializability, but bare function is not serializable
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

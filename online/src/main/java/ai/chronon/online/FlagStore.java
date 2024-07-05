package ai.chronon.online;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface to allow rolling out features/infrastructure changes in a safe & controlled manner.
 *
 * The "Flag"s in FlagStore referes to 'feature flags', a technique that allows enabling or disabling features at
 * runtime.
 *
 * Chronon users can provide their own implementation in the Api.
  */
public interface FlagStore extends Serializable {
    Boolean isSet(String flagName, Map<String, String> attributes);
}

package ai.chronon.online;

import java.io.Serializable;
import java.util.Map;

// Interface to allow rolling out features/infrastructure changes in a safe, controlled manner
public interface FlagStore extends Serializable {
    Boolean isSet(String flagName, Map<String, String> attributes);
}

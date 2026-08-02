package com.hurricache.client.intf;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public interface HurriCacheClientInterfaceCommon {
    /**
     * Gets the default client identifier used for routing and locking metadata.
     *
     * @return default client ID.
     */
    int getDefaultClientId();

    /**
     * Gets the default timeout duration applied to RPC invocations.
     *
     * @return default timeout as a {@link Duration}.
     */
    Duration getDefaultTimeout();

    /**
     * Gets the default Time-To-Live (TTL) duration applied to created entries.
     *
     * @return default TTL {@link Duration}, or {@code null} if entries do not expire by default.
     */
    default Duration getDefaultTtl() {
        return null;
    }

    /**
     * Gets the target server connection endpoint string.
     *
     * @return connection target (e.g., "host:port").
     */
    String getTarget();

    /**
     * Serializes a string key into a UTF-8 byte array representation.
     *
     * @param key target key string.
     * @return byte array representation of the key.
     */
    default byte[] serializeKey(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }
}
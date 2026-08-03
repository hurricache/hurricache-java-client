package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientHashMap extends HurriCacheClientInterfaceCommon, HurriCacheClientMapBased{
    /**
     * Creates an Unordered Map container using standard {@link Payload} entries.
     */
    CompletableFuture<KeyHintData> createMap(byte[] key,
                                             Map<Payload, Payload> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createMap(String key, Map<Payload, Payload> initialValue) {
        return createMap(serializeKey(key),
                         initialValue == null
                         ? Collections.emptyMap()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createMap(byte[] key, Map<Payload, Payload> initialValue) {
        return createMap(key,
                         initialValue == null
                         ? Collections.emptyMap()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    /**
     * Dumps or streams all entries from an unordered Map container.
     */
    CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Map<Payload, Payload>> streamMap(String key) {
        return streamMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Payload>> streamMap(String key, KeyHintData hint) {
        return streamMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint) {
        return streamMap(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
}
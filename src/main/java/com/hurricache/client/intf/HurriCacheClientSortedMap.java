package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSortedMap extends HurriCacheClientInterfaceCommon,HurriCacheClientMapBased{
    /**
     * Creates an OrderedMap container where keys are instance of {@link OrderedPayload}.
     */
    CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                    Map<OrderedPayload, Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedMap(String key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(serializeKey(key),
                                initialValue == null
                                ? Collections.emptyMap()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(key,
                                initialValue == null
                                ? Collections.emptyMap()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }


    /**
     * Streams or dumps all entries stored in an OrderedMap.
     */
    CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key,
                                                                     KeyHintData hint,
                                                                     int clientId,
                                                                     Duration timeout);

    default CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(String key) {
        return streamOrderedMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(String key, KeyHintData hint) {
        return streamOrderedMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

}
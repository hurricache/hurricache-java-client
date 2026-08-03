package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSortedSet extends HurriCacheClientInterfaceCommon{
    /**
     * Creates an OrderedSet container containing weight/score-ranked {@link OrderedPayload} elements.
     */
    CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                    List<OrderedPayload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedSet(String key, List<OrderedPayload> initialValue) {
        return createOrderedSet(serializeKey(key),
                                initialValue == null
                                ? Collections.emptyList()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedPayload> initialValue) {
        return createOrderedSet(key,
                                initialValue == null
                                ? Collections.emptyList()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }
}
package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSet extends HurriCacheClientInterfaceCommon{

    /**
     * Creates a Set container (unordered unique collection).
     */
    CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createSet(String key, List<Payload> initialValue) {
        return createSet(serializeKey(key),null , initialValue == null
                                                  ? Collections.emptyList()
                                                  : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createSet(byte[] key, List<Payload> initialValue) {
        return createSet(key,null , initialValue == null
                                    ? Collections.emptyList()
                                    : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
}
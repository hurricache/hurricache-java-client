package com.hurricache.client.intf;

import com.hurricache.grpc.KeyHint;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientQueue extends HurriCacheClientInterfaceCommon,HurriCacheClientDequeBased {
    /**
     * Creates a Queue container.
     *
     * @param key          target container key.
     * @param keyHint
     * @param initialValue initial sequence of {@link Payload} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} with created {@link KeyHint}.
     */
    CompletableFuture<KeyHintData> createQueue(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                               Duration ttl,
                                               int clientId,
                                               Duration timeout);

    default CompletableFuture<KeyHintData> createQueue(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createQueue(key,null , initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(String key) {
        return createQueue(serializeKey(key), null ,
                           Collections.emptyList(),
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(String key, List<Payload> initialValue) {
        return createQueue(serializeKey(key), null, initialValue == null
                                                    ? Collections.emptyList()
                                                    : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(byte[] key, List<Payload> initialValue) {
        return createQueue(key, null, initialValue == null
                                      ? Collections.emptyList()
                                      : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

}
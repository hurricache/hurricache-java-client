package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientVector  extends HurriCacheClientInterfaceCommon, HurriCacheClientDequeBased,HurriCacheClientRandomAccessContainers{

    /**
     * Creates a Vector container (dynamic indexed array).
     */
    CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createVector(key,null, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key) {
        return createVector(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Payload> initialValue) {
        return createVector(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Payload> initialValue) {
        return createVector(key,null , initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Payload> initialValue, int clientId) {
        return createVector(serializeKey(key),null , initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    /**
     * Streams or retrieves elements contained in a Vector container.
     */
    CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Payload>> streamVector(String key) {
        return streamVector(key, null);
    }

    default CompletableFuture<List<Payload>> streamVector(String key, KeyHintData hint) {
        return streamVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamVector(String key, int clientId) {
        return streamVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint) {
        return streamVector(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
}
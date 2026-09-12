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

    /**
     * Adds elements to an unordered container (e.g., Set,HashSet).
     */
    CompletableFuture<Integer> addElementUnordered(byte[] key,
                                                   KeyHintData hint,
                                                   List<Payload> data,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> addElementUnordered(String key, List<Payload> data) {
        return addElementUnordered(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint, List<Payload> data) {
        return addElementUnordered(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementUnordered(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Fetches a slice (range) of elements from an unordered container based on position indexes.
     */
    CompletableFuture<List<Payload>> streamSet(byte[] key, KeyHintData hint,int clientId,
                                               Duration timeout);

    default CompletableFuture<List<Payload>> streamSet(String key,
                                                                           KeyHintData hint) {
        return streamSet(serializeKey(key),
                                             hint,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamSet(String key,KeyHintData hint,
                                                                           int clientId) {
        return streamSet(serializeKey(key),
                                             hint,
                                             clientId,
                                             getDefaultTimeout());
    }


}
package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

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
    CompletableFuture<Boolean> addElement(byte[] key,
                                          KeyHintData hint,
                                          List<Payload> data,
                                          int clientId,
                                          Duration timeout);

    default CompletableFuture<Boolean> addElement(String key, List<Payload> data) {
        return addElement(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(String key, KeyHintData hint, List<Payload> data) {
        return addElement(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElement(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Fetches a slice (range) of elements from an unordered container based on position indexes.
     */
    CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                   KeyHintData hint,
                                                                   ContainerType containerType,
                                                                   int start,
                                                                   int end,
                                                                   int clientId,
                                                                   Duration timeout);

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end,
                                                                           int clientId) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             null,
                                             containerType,
                                             start,
                                             end,
                                             clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(key,
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }
}
package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSortedMap extends HurriCacheClientInterfaceCommon,HurriCacheClientMapBased,HurriCacheClientWeightBasedContainers{
    /**
     * Creates an OrderedMap container where keys are instance of {@link OrderedPayload}.
     */
    CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint, Map<OrderedPayload, Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedMap(String key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(serializeKey(key), null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(key, null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
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
    /**
     * Adds elements to an unordered container (e.g., Set,HashSet).
     */
    CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedPayload> data,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementWithWeight(String key, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint, List<OrderedPayload> data) {
        return addElementWithWeight(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Map<OrderedPayload,Payload>>  streamElementInRangeOrderedMap(byte[] key,
                                                                           KeyHintData hint,
                                                                           long startWeight,
                                                                           long endWeight,
                                                                           boolean reverse,
                                                                           int clientId,
                                                                           Duration timeout);
}
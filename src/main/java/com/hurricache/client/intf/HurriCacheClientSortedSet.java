package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

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

    /**
     * Fetches a sub-range of elements from an {@link OrderedSet} filtered by score/weight boundaries.
     *
     * @param startWeight lower bound weight limit.
     * @param endWeight   upper bound weight limit.
     * @param reverse     {@code true} for descending ordering, {@code false} for ascending.
     */
    CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key,
                                                                           KeyHintData hint,
                                                                           long startWeight,
                                                                           long endWeight,
                                                                           boolean reverse,
                                                                           int clientId,
                                                                           Duration timeout);

    default CompletableFuture<List<OrderedPayload>> streamElementInRangeOrdered(String key,
                                                                                ContainerType containerType,
                                                                                long startWeight,
                                                                                long endWeight) {
        return streamElementInRangeOrderedSet(serializeKey(key),
                                              null,
                                              startWeight,
                                              endWeight,
                                              false,
                                              getDefaultClientId(),
                                              getDefaultTimeout());
    }

    /**
     * Adds weighted elements to an {@link OrderedSet}.
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

}
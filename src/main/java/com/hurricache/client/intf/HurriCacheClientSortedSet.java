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
    CompletableFuture<KeyHintData> createOrderedSet(byte[] key, KeyHintData keyHint, List<OrderedPayload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedSet(String key, List<OrderedPayload> initialValue) {
        return createOrderedSet(serializeKey(key), null, initialValue == null
        ? Collections.emptyList()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedPayload> initialValue) {
        return createOrderedSet(key,null , initialValue == null
        ? Collections.emptyList()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
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
     * Fetches a slice (range) of elements from an unordered container based on position indexes.
     */
    CompletableFuture<List<OrderedPayload>> streamOrderedSet(byte[] key, KeyHintData hint,int clientId,
                                               Duration timeout);

    default CompletableFuture<List<OrderedPayload>> streamOrderedSet(String key,
                                                       KeyHintData hint) {
        return streamOrderedSet(serializeKey(key),
                         hint,
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedPayload>> streamOrderedSet(String key,KeyHintData hint,
                                                       int clientId) {
        return streamOrderedSet(serializeKey(key),
                         hint,
                         clientId,
                         getDefaultTimeout());
    }

}
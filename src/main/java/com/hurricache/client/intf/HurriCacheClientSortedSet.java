package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSortedSet extends HurriCacheClientInterfaceCommon{
    /**
     * Creates an OrderedSet container containing weight/score-ranked {@link OrderedVariant} elements.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial list of ordered payload elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createOrderedSet(byte[] key, KeyHintData keyHint, List<OrderedVariant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedSet(String key, KeyHintData keyHint,
                                                            List<OrderedVariant> initialValue,
                                                            Duration ttl, int clientId, Duration timeout) {
        return createOrderedSet(serializeKey(key), keyHint, initialValue, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createOrderedSet(String key, List<OrderedVariant> initialValue) {
        return createOrderedSet(serializeKey(key), null, initialValue == null
        ? Collections.emptyList()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedVariant> initialValue) {
        return createOrderedSet(key, null, initialValue == null
        ? Collections.emptyList()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }



    /**
     * Fetches a sub-range of elements from an {@link OrderedSet} filtered by score/weight boundaries.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param startWeight  lower bound weight limit.
     * @param endWeight    upper bound weight limit.
     * @param reverse      descending order if true.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing list of ordered payload elements in range.
     */
    CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key,
                                                                        KeyHintData hint,
                                                                        long startWeight,
                                                                        long endWeight,
                                                                        boolean reverse,
                                                                        int clientId,
                                                                        Duration timeout);

    default CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(String key,
                                                                                KeyHintData hint,
                                                                                long startWeight,
                                                                                long endWeight,
                                                                                boolean reverse,
                                                                                int clientId,
                                                                                Duration timeout) {
        return getElementInRangeOrderedSet(serializeKey(key), hint, startWeight, endWeight, reverse, clientId, timeout);
    }

    default CompletableFuture<List<OrderedVariant>> streamElementInRangeOrdered(String key,
                                                                                ContainerType containerType,
                                                                                long startWeight,
                                                                                long endWeight) {
        return getElementInRangeOrderedSet(serializeKey(key),
                                           null,
                                           startWeight,
                                           endWeight,
                                           false,
                                           getDefaultClientId(),
                                           getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key,
                                                                                KeyHintData hint,
                                                                                long startWeight,
                                                                                long endWeight,
                                                                                boolean reverse) {
        return getElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(String key,
                                                                                KeyHintData hint,
                                                                                long startWeight,
                                                                                long endWeight,
                                                                                boolean reverse) {
        return getElementInRangeOrderedSet(serializeKey(key), hint, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key,
                                                                                KeyHintData hint,
                                                                                long startWeight,
                                                                                long endWeight,
                                                                                boolean reverse,
                                                                                int clientId) {
        return getElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse,
                                           clientId, getDefaultTimeout());
    }

    /**
     * Fetches all elements from an OrderedSet container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing list of all ordered payload elements.
     */
    CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key, KeyHintData hint, int clientId,
                                                          Duration timeout);

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(String key, KeyHintData hint, int clientId,
                                                                  Duration timeout) {
        return getOrderedSet(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(String key) {
        return getOrderedSet(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key) {
        return getOrderedSet(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(String key, KeyHintData hint) {
        return getOrderedSet(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key, KeyHintData hint) {
        return getOrderedSet(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(String key, KeyHintData hint, int clientId) {
        return getOrderedSet(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key, KeyHintData hint, int clientId) {
        return getOrderedSet(key, hint, clientId, getDefaultTimeout());
    }
}
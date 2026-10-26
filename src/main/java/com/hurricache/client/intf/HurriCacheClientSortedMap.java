package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSortedMap extends HurriCacheClientInterfaceCommon,HurriCacheClientMapBased,HurriCacheClientWeightBasedContainers{
    /**
     * Creates an OrderedMap container where keys are instance of {@link OrderedPayload}.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial map of ordered entries.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint, Map<OrderedPayload, Variant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedMap(String key, KeyHintData keyHint,
                                                            Map<OrderedPayload, Variant> initialValue,
                                                            Duration ttl, int clientId, Duration timeout) {
        return createOrderedMap(serializeKey(key), keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createOrderedMap(String key, KeyHintData keyHint,
                                                            Map<OrderedPayload, Variant> initialValue,
                                                            int clientId) {
        return createOrderedMap(serializeKey(key), keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(String key, KeyHintData keyHint,
                                                            Map<OrderedPayload, Variant> initialValue) {
        return createOrderedMap(serializeKey(key), keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(String key, Map<OrderedPayload, Variant> initialValue) {
        return createOrderedMap(serializeKey(key), null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint,
                                                            Map<OrderedPayload, Variant> initialValue,
                                                            int clientId) {
        return createOrderedMap(key, keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint,
                                                            Map<OrderedPayload, Variant> initialValue) {
        return createOrderedMap(key, keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Variant> initialValue) {
        return createOrderedMap(key, null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams or dumps all entries stored in an OrderedMap.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing all ordered map entries.
     */
    CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key,
                                                                  KeyHintData hint,
                                                                  int clientId,
                                                                  Duration timeout);

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(String key, KeyHintData hint,
                                                                          int clientId, Duration timeout) {
        return getOrderedMap(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(String key) {
        return getOrderedMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key) {
        return getOrderedMap(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(String key, KeyHintData hint) {
        return getOrderedMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key, KeyHintData hint) {
        return getOrderedMap(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(String key, KeyHintData hint, int clientId) {
        return getOrderedMap(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key, KeyHintData hint, int clientId) {
        return getOrderedMap(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Dumps all entries from an OrderedMap with an explicit client identifier and no routing hint.
     *
     * @param key      target container key as a string (UTF-8 encoded).
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing all ordered map entries.
     */
    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(String key, int clientId) {
        return getOrderedMap(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Dumps all entries from an OrderedMap with an explicit client identifier and no routing hint.
     *
     * @param key      target container key in byte array form.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing all ordered map entries.
     */
    default CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key, int clientId) {
        return getOrderedMap(key, null, clientId, getDefaultTimeout());
    }

    /**
     * Adds elements to an ordered map container.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param container_keys list of ordered payload keys.
     * @param container_values list of associated Variant values.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementOrderedMap(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedPayload> container_keys,
                                                    List<Variant> container_values,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementOrderedMap(String key, KeyHintData hint,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values,
                                                            int clientId, Duration timeout) {
        return addElementOrderedMap(serializeKey(key), hint, container_keys, container_values, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementOrderedMap(String key, KeyHintData hint,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values,
                                                            int clientId) {
        return addElementOrderedMap(serializeKey(key), hint, container_keys, container_values,
                                    clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementOrderedMap(String key, KeyHintData hint,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(serializeKey(key), hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementOrderedMap(String key, List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(serializeKey(key), null, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementOrderedMap(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values,
                                                            int clientId) {
        return addElementOrderedMap(key, hint, container_keys, container_values,
                                    clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementOrderedMap(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(key, hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementOrderedMap(byte[] key, List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(key, null, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an ordered map container.
     * <p>
     * Legacy overload with {@code clientId} placed before the element lists,
     * kept for backward compatibility.
     *
     * @param key              target container key as a string (UTF-8 encoded).
     * @param hint             optional key routing hint.
     * @param clientId         identifier of the issuing client.
     * @param container_keys   list of ordered payload keys.
     * @param container_values list of associated Variant values.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementOrderedMap(String key, KeyHintData hint, int clientId,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(serializeKey(key), hint, container_keys, container_values,
                                    clientId, getDefaultTimeout());
    }

    /**
     * Adds elements to an ordered map container.
     * <p>
     * Legacy overload with {@code clientId} placed before the element lists,
     * kept for backward compatibility.
     *
     * @param key              target container key in byte array form.
     * @param hint             optional key routing hint.
     * @param clientId         identifier of the issuing client.
     * @param container_keys   list of ordered payload keys.
     * @param container_values list of associated Variant values.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementOrderedMap(byte[] key, KeyHintData hint, int clientId,
                                                            List<OrderedPayload> container_keys,
                                                            List<Variant> container_values) {
        return addElementOrderedMap(key, hint, container_keys, container_values,
                                    clientId, getDefaultTimeout());
    }



    /**
     * Streams elements in a range from an OrderedMap by weight/score boundaries.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param startWeight  lower bound weight.
     * @param endWeight    upper bound weight.
     * @param reverse      descending order if true.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing matching map entries.
     */
    CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                               KeyHintData hint,
                                                                               long startWeight,
                                                                               long endWeight,
                                                                               boolean reverse,
                                                                               int clientId,
                                                                               Duration timeout);

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(String key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId,
                                                                                       Duration timeout) {
        return getElementInRangeOrderedMap(serializeKey(key), hint, startWeight, endWeight, reverse, clientId, timeout);
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(String key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId) {
        return getElementInRangeOrderedMap(serializeKey(key), hint, startWeight, endWeight, reverse,
                                           clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(String key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse) {
        return getElementInRangeOrderedMap(serializeKey(key), hint, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(String key,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse) {
        return getElementInRangeOrderedMap(serializeKey(key), null, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId) {
        return getElementInRangeOrderedMap(key, hint, startWeight, endWeight, reverse,
                                           clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse) {
        return getElementInRangeOrderedMap(key, hint, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload,Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse) {
        return getElementInRangeOrderedMap(key, null, startWeight, endWeight, reverse,
                                           getDefaultClientId(), getDefaultTimeout());
    }
}

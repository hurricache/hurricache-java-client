package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientSet extends HurriCacheClientInterfaceCommon{

    /**
     * Creates a Set container (unordered unique collection).
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Payload} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createSet(String key, List<Payload> initialValue) {
        return createSet(serializeKey(key), null, initialValue == null
                                                  ? Collections.emptyList()
                                                  : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createSet(byte[] key, List<Payload> initialValue) {
        return createSet(key, null, initialValue == null
                                    ? Collections.emptyList()
                                    : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container (e.g., Set, HashSet).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link Payload} elements to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    CompletableFuture<Integer> addElementUnordered(byte[] key,
                                                   KeyHintData hint,
                                                   List<Payload> data,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> addElementUnordered(String key, List<Payload> data) {
        return addElementUnordered(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(byte[] key, List<Payload> data) {
        return addElementUnordered(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint, List<Payload> data) {
        return addElementUnordered(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementUnordered(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint, List<Payload> data,
                                                           int clientId) {
        return addElementUnordered(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Payload> data,
                                                           int clientId) {
        return addElementUnordered(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing list of all {@link Payload} elements.
     */
    CompletableFuture<List<Payload>> streamSet(byte[] key, KeyHintData hint, int clientId,
                                               Duration timeout);

    default CompletableFuture<List<Payload>> streamSet(String key, KeyHintData hint) {
        return streamSet(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamSet(String key, KeyHintData hint, int clientId) {
        return streamSet(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamSet(byte[] key, KeyHintData hint, int clientId) {
        return streamSet(key, hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamSet(String key) {
        return streamSet(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamSet(byte[] key) {
        return streamSet(key, null, getDefaultClientId(), getDefaultTimeout());
    }
}
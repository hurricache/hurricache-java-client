package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientVector  extends HurriCacheClientInterfaceCommon, HurriCacheClientDequeBased,HurriCacheClientRandomAccessContainers{

    /**
     * Creates a Vector container (dynamic indexed array).
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Variant> initialValue,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    /**
     * Creates a Vector container (dynamic indexed array).
     *
     * @param key          target container key in String form (UTF-8 encoded).
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createVector(String key, KeyHintData keyHint, List<Variant> initialValue,
                                                        Duration ttl, int clientId, Duration timeout) {
        return createVector(serializeKey(key), keyHint, initialValue, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Variant> initialValue, Duration ttl) {
        return createVector(key, null, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key) {
        return createVector(serializeKey(key), null, Collections.emptyList(), getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Variant> initialValue) {
        return createVector(key, null, initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Variant> initialValue) {
        return createVector(serializeKey(key), null, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key, KeyHintData keyHint, List<Variant> initialValue) {
        return createVector(serializeKey(key), keyHint, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Creates a Vector container with the default TTL and default execution timeout.
     *
     * @param key          target container key in String form (UTF-8 encoded).
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param clientId     identifier of the issuing client.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createVector(String key, KeyHintData keyHint, List<Variant> initialValue,
                                                        int clientId) {
        return createVector(serializeKey(key), keyHint, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Creates a Vector container with the default TTL and default execution timeout.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param clientId     identifier of the issuing client.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Variant> initialValue,
                                                        int clientId) {
        return createVector(key, keyHint, initialValue == null
                                          ? Collections.emptyList()
                                          : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Creates a Vector container with the default TTL, default client id and default execution timeout.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Variant> initialValue) {
        return createVector(key, keyHint, initialValue == null
                                          ? Collections.emptyList()
                                          : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Variant> initialValue, int clientId) {
        return createVector(serializeKey(key), null, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Streams or retrieves elements contained in a Vector container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Variant>> getVector(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getVector(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<List<Variant>> getVector(String key) {
        return getVector(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(byte[] key) {
        return getVector(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(String key, KeyHintData hint) {
        return getVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint) {
        return getVector(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(String key, int clientId) {
        return getVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(byte[] key, int clientId) {
        return getVector(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(String key, KeyHintData hint, int clientId) {
        return getVector(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint, int clientId) {
        return getVector(key, hint, clientId, getDefaultTimeout());
    }
}
package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.hurricache.client.intf.Variant;

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
    CompletableFuture<List<Variant>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Variant>> streamVector(String key, KeyHintData hint, int clientId, Duration timeout) {
        return streamVector(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<List<Variant>> streamVector(String key) {
        return streamVector(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamVector(byte[] key) {
        return streamVector(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamVector(String key, KeyHintData hint) {
        return streamVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamVector(byte[] key, KeyHintData hint) {
        return streamVector(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamVector(String key, int clientId) {
        return streamVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamVector(byte[] key, int clientId) {
        return streamVector(key, null, clientId, getDefaultTimeout());
    }
}
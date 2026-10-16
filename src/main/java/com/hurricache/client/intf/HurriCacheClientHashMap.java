package com.hurricache.client.intf;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientHashMap extends HurriCacheClientInterfaceCommon, HurriCacheClientMapBased{
    /**
     * Creates an Unordered Map container using standard {@link Payload} entries.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial map of entries.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createMap(byte[] key, KeyHintData keyHint, Map<Payload, Variant> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createMap(String key, KeyHintData keyHint, Map<Payload, Variant> initialValue,
                                                     Duration ttl, int clientId, Duration timeout) {
        return createMap(serializeKey(key), keyHint, initialValue == null
        ? Collections.emptyMap()
        : initialValue, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createMap(String key, Map<Payload, Variant> initialValue) {
        return createMap(serializeKey(key), null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createMap(byte[] key, Map<Payload, Variant> initialValue) {
        return createMap(key, null, initialValue == null
        ? Collections.emptyMap()
        : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Dumps or streams all entries from an unordered Map container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing all map entries.
     */
    CompletableFuture<Map<Payload, Variant>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Map<Payload, Variant>> streamMap(String key, KeyHintData hint, int clientId,
                                                               Duration timeout) {
        return streamMap(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(String key) {
        return streamMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(byte[] key) {
        return streamMap(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(String key, KeyHintData hint) {
        return streamMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(String key, KeyHintData hint, int clientId) {
        return streamMap(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(byte[] key, KeyHintData hint) {
        return streamMap(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(byte[] key, KeyHintData hint, int clientId) {
        return streamMap(key, hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(String key, int clientId) {
        return streamMap(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Variant>> streamMap(byte[] key, int clientId) {
        return streamMap(key, null, clientId, getDefaultTimeout());
    }
}
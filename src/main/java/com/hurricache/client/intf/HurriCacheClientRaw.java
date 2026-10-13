package com.hurricache.client.intf;

import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.KeyHint;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientRaw extends HurriCacheClientInterfaceCommon{
    /**
     * Atomically retrieves the value associated with the key and removes the key from the cache.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing a {@link Variant} with the deleted value, or {@code null} if not found.
     */
    CompletableFuture<Variant> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getAndDeleteValue(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getAndDeleteValue(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, getDefaultClientId());
    }

    default CompletableFuture<Variant> getAndDeleteValue(byte[] key) {
        return getAndDeleteValue(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(String key, KeyHintData hint) {
        return getAndDeleteValue(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(byte[] key, KeyHintData hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(byte[] key, int clientId) {
        return getAndDeleteValue(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(String key, KeyHintData hint, int clientId) {
        return getAndDeleteValue(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId) {
        return getAndDeleteValue(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Creates a standard Key-Value entry in the cache storage.
     * The value can be bytes, int, or double depending on the {@link Variant} type.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    payload to store (Variant: bytes, int, or double).
     * @param ttl      entry expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing assigned or calculated {@link KeyHint}.
     */
    CompletableFuture<KeyHintData> createKeyValue(byte[] key,
                                                  KeyHintData hint,
                                                  Variant value,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout);

    default CompletableFuture<KeyHintData> createKeyValue(String key, KeyHintData hint, Variant value,
                                                         Duration ttl, int clientId, Duration timeout) {
        return createKeyValue(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, Variant value, int clientId, Duration timeout) {
        return createKeyValue(key, null, value, getDefaultTtl(), clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createKeyValue(String key, Variant value) {
        return createKeyValue(serializeKey(key), value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, Variant value) {
        return createKeyValue(key, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(String key, KeyHintData hint, Variant value, int clientId) {
        return createKeyValue(serializeKey(key), hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, KeyHintData hint, Variant value, int clientId) {
        return createKeyValue(key, hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(String key, Variant value, int clientId) {
        return createKeyValue(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, Variant value, int clientId) {
        return createKeyValue(key, value, clientId, getDefaultTimeout());
    }

    /**
     * Fetches payload value associated with the specified key.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} with a {@link Variant} containing the value, or {@code null} if the key does not exist.
     */
    CompletableFuture<Variant> getValue(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getValue(String key) {
        return getValue(key, getDefaultClientId());
    }

    default CompletableFuture<Variant> getValue(byte[] key) {
        return getValue(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(String key, KeyHintData hint) {
        return getValue(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(byte[] key, KeyHintData hint) {
        return getValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(String key, int clientId) {
        return getValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(byte[] key, int clientId) {
        return getValue(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(String key, KeyHintData hint, int clientId) {
        return getValue(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValue(byte[] key, KeyHintData hint, int clientId) {
        return getValue(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Replaces payload value of an existing Key-Value entry.
     * The value can be bytes, int, or double depending on the {@link Variant} type.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new payload (Variant: bytes, int, or double).
     * @param ttl      updated TTL duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} with a {@link Variant} containing the previous value if configured, or acknowledgment.
     */
    CompletableFuture<Variant> updateKeyValue(byte[] key,
                                             KeyHintData hint,
                                             Variant value,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<Variant> updateKeyValue(String key, KeyHintData hint, Variant value,
                                                     Duration ttl, int clientId, Duration timeout) {
        return updateKeyValue(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<Variant> updateKeyValue(byte[] key, KeyHintData hint, Variant value) {
        return updateKeyValue(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateKeyValue(String key, Variant value) {
        return updateKeyValue(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateKeyValue(byte[] key, KeyHintData keyHint, Variant value, int clientId) {
        return updateKeyValue(key, keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateKeyValue(String key, KeyHintData keyHint, Variant value, int clientId) {
        return updateKeyValue(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateKeyValue(String key, KeyHintData hint, Variant value) {
        return updateKeyValue(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateKeyValue(String key, Variant value, int clientId) {
        return updateKeyValue(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Checks whether the specified key exists in the storage.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} returning {@code true} if key exists, {@code false} otherwise.
     */
    CompletableFuture<Boolean> existKey(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> existKey(String key, KeyHintData hint, int clientId, Duration timeout) {
        return existKey(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Boolean> existKey(String key) {
        return existKey(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key) {
        return existKey(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, KeyHintData hint) {
        return existKey(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key, KeyHintData hint) {
        return existKey(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, int clientId) {
        return existKey(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key, int clientId) {
        return existKey(key, null, clientId, getDefaultTimeout());
    }
}
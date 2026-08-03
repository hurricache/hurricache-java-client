package com.hurricache.client.intf;

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
     * @return a {@link CompletableFuture} containing deleted value bytes, or {@code null} if not found.
     */
    CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHintData hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHintData hint, int clientId) {
        return getAndDeleteValue(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    /**
     * Creates a standard Key-Value entry in the cache storage.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    payload bytes to store.
     * @param ttl      entry expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing assigned or calculated {@link KeyHint}.
     */
    CompletableFuture<KeyHintData> createKeyValue(byte[] key,
                                                  KeyHintData hint,
                                                  byte[] value,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout);

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, byte[] value, int clientId, Duration timeout) {
        return createKeyValue(key, null, value, getDefaultTtl(), clientId, timeout);
    }

    default CompletableFuture<KeyHintData> createKeyValue(String key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId());
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(String key, byte[] value, int clientId) {
        return createKeyValue(serializeKey(key), value, clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createKeyValue(byte[] key, byte[] value, int clientId) {
        return createKeyValue(key, value, clientId, getDefaultTimeout());
    }

    /**
     * Fetches payload value associated with the specified key.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} with value bytes, or {@code null} if the key does not exist.
     */
    CompletableFuture<byte[]> getValue(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getValue(String key) {
        return getValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getValue(byte[] key) {
        return getValue(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHintData hint) {
        return getValue(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(byte[] key, KeyHintData hint) {
        return getValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, int clientId) {
        return getValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(byte[] key, KeyHintData keyhint, int clientId) {
        return getValue(key, keyhint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHintData keyhint, int clientId) {
        return getValue(serializeKey(key), keyhint, clientId, getDefaultTimeout());
    }

    /**
     * Replaces payload value of an existing Key-Value entry.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new payload bytes.
     * @param ttl      updated TTL duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} with previous value bytes if configured, or acknowledgment bytes.
     */
    CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                             KeyHintData hint,
                                             byte[] value,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHintData hint, byte[] value) {
        return updateKeyValue(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value) {
        return updateKeyValue(serializeKey(key),
                              null,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHintData keyHint, byte[] value, int clientID) {
        return updateKeyValue(key, keyHint, value, getDefaultTtl(), clientID, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHintData keyHint, byte[] value, int clientID) {
        return updateKeyValue(serializeKey(key), keyHint, value, getDefaultTtl(), clientID, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHintData hint, byte[] value) {
        return updateKeyValue(serializeKey(key),
                              hint,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value, int clientId) {
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



}
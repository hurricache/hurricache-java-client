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
     * @return a {@link CompletableFuture} containing a {@link Variant} with the deleted value, or {@code null} if not found.
     */
    CompletableFuture<Variant> pop(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> pop(String key, KeyHintData hint, int clientId, Duration timeout) {
        return pop(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> pop(String key) {
        return pop(key, getDefaultClientId());
    }

    default CompletableFuture<Variant> pop(byte[] key) {
        return pop(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(String key, KeyHintData hint) {
        return pop(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(byte[] key, KeyHintData hint) {
        return pop(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(String key, int clientId) {
        return pop(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(byte[] key, int clientId) {
        return pop(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(String key, KeyHintData hint, int clientId) {
        return pop(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> pop(byte[] key, KeyHintData hint, int clientId) {
        return pop(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<KeyHintData> create(byte[] key,
                                          KeyHintData hint,
                                          Variant value,
                                          Duration ttl,
                                          int clientId,
                                          Duration timeout);

    default CompletableFuture<KeyHintData> create(String key, KeyHintData hint, Variant value,
                                                  Duration ttl, int clientId, Duration timeout) {
        return create(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> create(byte[] key, Variant value, int clientId, Duration timeout) {
        return create(key, null, value, getDefaultTtl(), clientId, timeout);
    }

    default CompletableFuture<KeyHintData> create(String key, Variant value) {
        return create(serializeKey(key), value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> create(byte[] key, Variant value) {
        return create(key, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> create(String key, KeyHintData hint, Variant value, int clientId) {
        return create(serializeKey(key), hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> create(byte[] key, KeyHintData hint, Variant value, int clientId) {
        return create(key, hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> create(String key, Variant value, int clientId) {
        return create(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> create(byte[] key, Variant value, int clientId) {
        return create(key, value, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> get(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> get(String key) {
        return get(key, getDefaultClientId());
    }

    default CompletableFuture<Variant> get(byte[] key) {
        return get(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(String key, KeyHintData hint) {
        return get(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(byte[] key, KeyHintData hint) {
        return get(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(String key, int clientId) {
        return get(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(byte[] key, int clientId) {
        return get(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(String key, KeyHintData hint, int clientId) {
        return get(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> get(byte[] key, KeyHintData hint, int clientId) {
        return get(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> replace(byte[] key,
                                       KeyHintData hint,
                                       Variant value,
                                       Duration ttl,
                                       int clientId,
                                       Duration timeout);

    default CompletableFuture<Variant> replace(String key, KeyHintData hint, Variant value,
                                               Duration ttl, int clientId, Duration timeout) {
        return replace(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<Variant> replace(byte[] key, KeyHintData hint, Variant value) {
        return replace(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> replace(String key, Variant value) {
        return replace(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> replace(byte[] key, KeyHintData keyHint, Variant value, int clientId) {
        return replace(key, keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> replace(String key, KeyHintData keyHint, Variant value, int clientId) {
        return replace(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> replace(String key, KeyHintData hint, Variant value) {
        return replace(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> replace(String key, Variant value, int clientId) {
        return replace(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
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
    CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> containsKey(String key, KeyHintData hint, int clientId, Duration timeout) {
        return containsKey(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Boolean> containsKey(String key) {
        return containsKey(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(byte[] key) {
        return containsKey(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(String key, KeyHintData hint) {
        return containsKey(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint) {
        return containsKey(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(String key, int clientId) {
        return containsKey(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(byte[] key, int clientId) {
        return containsKey(key, null, clientId, getDefaultTimeout());
    }
}
package com.hurricache.client.intf;

import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientInterfaceCommon {
    /**
     * Gets the default client identifier used for routing and locking metadata.
     *
     * @return default client ID.
     */
    int getDefaultClientId();

    /**
     * Gets the default timeout duration applied to RPC invocations.
     *
     * @return default timeout as a {@link Duration}.
     */
    Duration getDefaultTimeout();

    /**
     * Gets the default Time-To-Live (TTL) duration applied to created entries.
     *
     * @return default TTL {@link Duration}, or {@code null} if entries do not expire by default.
     */
    default Duration getDefaultTtl() {
        return null;
    }

    /**
     * Gets the target server connection endpoint string.
     *
     * @return connection target (e.g., "host:port").
     */
    String getTarget();

    /**
     * Serializes a string key into a UTF-8 byte array representation.
     *
     * @param key target key string.
     * @return byte array representation of the key.
     */
    default byte[] serializeKey(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }
    /**
     * Deletes an object associated with the specified key.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} returning {@code true} if deletion succeeded.
     */
    CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> remove(String key) {
        return remove(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHintData hint) {
        return remove(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, int clientId) {
        return remove(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint) {
        return remove(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientid) {
        return remove(key, hint, clientid, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHintData hint, int clientid) {
        return remove(serializeKey(key), hint, clientid, getDefaultTimeout());
    }

    /**
     * Queries current element count / size of a container data structure.
     */
    CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Integer> getSize(String key) {
        return getSize(key, getDefaultClientId());
    }

    default CompletableFuture<Integer> getSize(String key, int clientId) {
        return getSize(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> getSize(String key, KeyHintData hint) {
        return getSize(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint) {
        return getSize(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Acquires a distributed lock on an object key.
     *
     * @param key      target lock key.
     * @param hint     optional key routing hint.
     * @param type     type of lock requested (e.g., READ, WRITE).
     * @param clientId identifier of the acquiring client.
     * @param duration lease duration of the lock.
     * @param timeout  execution timeout duration.
     * @return resulting {@link LockStatus}.
     */
    CompletableFuture<LockStatus> lockObject(byte[] key,
                                             KeyHintData hint,
                                             LockType type,
                                             int clientId,
                                             Duration duration,
                                             Duration timeout);

    default CompletableFuture<LockStatus> lockObject(byte[] key, KeyHintData hint, LockType type, Duration duration) {
        return lockObject(key, hint, type, getDefaultClientId(), duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key,
                                                     KeyHintData hint,
                                                     LockType type,
                                                     int clientId,
                                                     Duration duration) {
        return lockObject(serializeKey(key), hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(byte[] key,
                                                     KeyHintData hint,
                                                     LockType type,
                                                     int clientId,
                                                     Duration duration) {
        return lockObject(key, hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, int clientId, Duration duration) {
        return lockObject(serializeKey(key), null, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, Duration duration) {
        return lockObject(serializeKey(key), null, type, getDefaultClientId(), duration, getDefaultTimeout());
    }

    /**
     * Releases a held lock on a specified key.
     */
    CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<LockStatus> unlockObject(String key, KeyHintData hint, int clientId) {
        return unlockObject(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint) {
        return unlockObject(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint, int clientid) {
        return unlockObject(key, hint, clientid, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key, int clientId) {
        return unlockObject(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key) {
        return unlockObject(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Updates or sets the Time-To-Live (TTL) for an existing key.
     *
     * @param key      target key in byte array form.
     * @param hint     optional routing hint for partition key localization.
     * @param ttl      TTL value in milliseconds/seconds as configured by protocol.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration for this RPC call.
     * @return a {@link CompletableFuture} resolving to {@code true} if TTL was successfully set.
     */
    CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId, Duration timeout);

    default CompletableFuture<Boolean> setTtl(String key, KeyHintData hint, long ttl) {
        return setTtl(key, hint, ttl, getDefaultClientId());
    }

    default CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId) {
        return setTtl(key, hint, ttl, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> setTtl(String key, KeyHintData hint, long ttl, int clientId) {
        return setTtl(serializeKey(key), hint, ttl, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl) {
        return setTtl(key, hint, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves the remaining TTL duration for the specified key.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} returning remaining TTL in milliseconds, or {@code -1} if non-expiring.
     */
    CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> getTtl(String key) {
        return getTtl(key, getDefaultClientId());
    }

    default CompletableFuture<Long> getTtl(String key, KeyHintData hint) {
        return getTtl(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint) {
        return getTtl(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(String key, int clientId) {
        return getTtl(serializeKey(key), null, clientId, getDefaultTimeout());
    }
}
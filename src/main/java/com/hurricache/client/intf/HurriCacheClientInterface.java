package com.hurricache.client.intf;

import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous client interface for interacting with the HurriCache distributed caching system.
 * <p>
 * Provides high-performance async methods for managing atomic counters/values, standard Key-Value pairs,
 * distributed locks, as well as ordered ({@link OrderedPayload}) and unordered ({@link Payload})
 * data structures (Queue, List, Vector, Set, Map).
 */
public interface HurriCacheClientInterface {

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

    // =========================================================================
    // TTL & BASIC KEY-VALUE OPERATIONS
    // =========================================================================

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

    // =========================================================================
    // UNORDERED CONTAINERS (Payload)
    // =========================================================================

    /**
     * Creates a Queue container.
     *
     * @param key          target container key.
     * @param initialValue initial sequence of {@link Payload} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} with created {@link KeyHint}.
     */
    CompletableFuture<KeyHintData> createQueue(byte[] key,
                                               List<Payload> initialValue,
                                               Duration ttl,
                                               int clientId,
                                               Duration timeout);

    default CompletableFuture<KeyHintData> createQueue(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createQueue(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(String key) {
        return createQueue(serializeKey(key),
                           Collections.emptyList(),
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(String key, List<Payload> initialValue) {
        return createQueue(serializeKey(key),
                           initialValue == null
                           ? Collections.emptyList()
                           : initialValue,
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createQueue(byte[] key, List<Payload> initialValue) {
        return createQueue(key,
                           initialValue == null
                           ? Collections.emptyList()
                           : initialValue,
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    /**
     * Creates a List container (linked sequence).
     */
    CompletableFuture<KeyHintData> createList(byte[] key,
                                              List<Payload> initialValue,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout);

    default CompletableFuture<KeyHintData> createList(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createList(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createList(String key) {
        return createList(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHintData> createList(String key, List<Payload> initialValue) {
        return createList(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHintData> createList(byte[] key, List<Payload> initialValue) {
        return createList(key, initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createList(String key, List<Payload> initialValue, int clientId) {
        return createList(serializeKey(key),
                          initialValue == null
                          ? Collections.emptyList()
                          : initialValue,
                          getDefaultTtl(),
                          clientId,
                          getDefaultTimeout());
    }

    /**
     * Creates a Vector container (dynamic indexed array).
     */
    CompletableFuture<KeyHintData> createVector(byte[] key,
                                                List<Payload> initialValue,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createVector(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key) {
        return createVector(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Payload> initialValue) {
        return createVector(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHintData> createVector(byte[] key, List<Payload> initialValue) {
        return createVector(key, initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createVector(String key, List<Payload> initialValue, int clientId) {
        return createVector(serializeKey(key),
                            initialValue == null
                            ? Collections.emptyList()
                            : initialValue,
                            getDefaultTtl(),
                            clientId,
                            getDefaultTimeout());
    }

    /**
     * Creates a Set container (unordered unique collection).
     */
    CompletableFuture<KeyHintData> createSet(byte[] key,
                                             List<Payload> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createSet(String key, List<Payload> initialValue) {
        return createSet(serializeKey(key),
                         initialValue == null
                         ? Collections.emptyList()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createSet(byte[] key, List<Payload> initialValue) {
        return createSet(key,
                         initialValue == null
                         ? Collections.emptyList()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    /**
     * Creates an Unordered Map container using standard {@link Payload} entries.
     */
    CompletableFuture<KeyHintData> createMap(byte[] key,
                                             Map<Payload, Payload> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    default CompletableFuture<KeyHintData> createMap(String key, Map<Payload, Payload> initialValue) {
        return createMap(serializeKey(key),
                         initialValue == null
                         ? Collections.emptyMap()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createMap(byte[] key, Map<Payload, Payload> initialValue) {
        return createMap(key,
                         initialValue == null
                         ? Collections.emptyMap()
                         : initialValue,
                         getDefaultTtl(),
                         getDefaultClientId(),
                         getDefaultTimeout());
    }

    /**
     * Atomically retrieves and removes the front element of a container (Pop Front / Dequeue).
     */
    CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveFront(String key) {
        return getAndRemoveFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, KeyHintData hint) {
        return getAndRemoveFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, int clientId) {
        return getAndRemoveFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Reads the front element of a container without modifying structure (Peek Front).
     */
    CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getFront(String key) {
        return getFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(byte[] key) {
        return getFront(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, KeyHintData hint) {
        return getFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, KeyHintData hint, int clientId) {
        return getFront(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, int clientId) {
        return getFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint) {
        return getFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Gets the element at the head of a sequence.
     */
    CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getHead(String key) {
        return getHead(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(String key, KeyHintData hint) {
        return getHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Gets the element at the tail of a sequence.
     */
    CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getTail(String key) {
        return getTail(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getTail(String key, KeyHintData hint) {
        return getTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Atomically retrieves and removes the tail element of a container (Pop Back).
     */
    CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveTail(String key) {
        return getAndRemoveTail(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, KeyHintData hint) {
        return getAndRemoveTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint) {
        return getAndRemoveTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, int clientId) {
        return getAndRemoveTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Appends a collection of elements to the tail of a container (Push Back).
     */
    CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToTail(String key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(String key,
                                                        KeyHintData hint,
                                                        List<Payload> data,
                                                        int clientId) {
        return addElementToTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Prepends a collection of elements to the head of a container (Push Front).
     */
    CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToHead(String key, List<Payload> data) {
        return addElementToHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(String key, KeyHintData hint, List<Payload> data) {
        return addElementToHead(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHintData keyHint, List<Payload> data) {
        return addElementToHead(key, keyHint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container (e.g., HashMap).
     */
    CompletableFuture<Integer> addElementHashMap(byte[] key,
                                          KeyHintData hint,
                                          List<Payload> container_keys,
                                          List<Payload> container_values,
                                          int clientId,
                                          Duration timeout);

    /**
     * Adds elements to an unordered container (e.g., OederedMap).
     */
    CompletableFuture<Integer> addElementOrderedMap(byte[] key,
                                          KeyHintData hint,
                                          List<OrderedPayload> container_keys,
                                          List<Payload> container_values,
                                          int clientId,
                                          Duration timeout);

    /**
     * Adds elements to an unordered container (e.g., Set,HashSet).
     */
    CompletableFuture<Boolean> addElement(byte[] key,
                                          KeyHintData hint,
                                          List<Payload> data,
                                          int clientId,
                                          Duration timeout);

    default CompletableFuture<Boolean> addElement(String key, List<Payload> data) {
        return addElement(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(String key, KeyHintData hint, List<Payload> data) {
        return addElement(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElement(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves an element located at a specific index/position.
     */
    CompletableFuture<Payload> getElementAtPosition(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, KeyHintData hint, int pos) {
        return getElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element situated at a specific index/position.
     */
    CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts elements at a target index/position within a container.
     */
    CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                    KeyHintData hint,
                                                    List<Payload> data,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos, int clientId) {
        return addElementToPosition(serializeKey(key), null, data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts elements immediately before a specified pivot element.
     */
    CompletableFuture<Boolean> addElementToPositionBefore(byte[] key,
                                                          KeyHintData hint,
                                                          List<Payload> data,
                                                          Payload pivot,
                                                          int clientId,
                                                          Duration timeout);

    default CompletableFuture<Boolean> addElementToPositionBefore(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionBefore(serializeKey(key),
                                          null,
                                          data,
                                          pivot,
                                          getDefaultClientId(),
                                          getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(String key,
                                                                  KeyHintData hint,
                                                                  List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(serializeKey(key),
                                          hint,
                                          data,
                                          pivot,
                                          getDefaultClientId(),
                                          getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(String key,
                                                                  List<Payload> data,
                                                                  Payload pivot,
                                                                  int clientId) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(byte[] key,
                                                                  KeyHintData hint,
                                                                  List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts elements immediately after a specified pivot element.
     */
    CompletableFuture<Boolean> addElementToPositionAfter(byte[] key,
                                                         KeyHintData hint,
                                                         List<Payload> data,
                                                         Payload pivot,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<Boolean> addElementToPositionAfter(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionAfter(serializeKey(key),
                                         null,
                                         data,
                                         pivot,
                                         getDefaultClientId(),
                                         getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(String key,
                                                                 KeyHintData hint,
                                                                 List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(serializeKey(key),
                                         hint,
                                         data,
                                         pivot,
                                         getDefaultClientId(),
                                         getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(String key,
                                                                 List<Payload> data,
                                                                 Payload pivot,
                                                                 int clientId) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(byte[] key,
                                                                 KeyHintData hint,
                                                                 List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams or retrieves all elements contained in a List container.
     */
    CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Payload>> streamList(String key) {
        return streamList(key, getDefaultClientId());
    }

    default CompletableFuture<List<Payload>> streamList(String key, int clientId) {
        return streamList(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamList(String key, KeyHintData hint) {
        return streamList(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint) {
        return streamList(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams or retrieves elements contained in a Vector container.
     */
    CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Payload>> streamVector(String key) {
        return streamVector(key, null);
    }

    default CompletableFuture<List<Payload>> streamVector(String key, KeyHintData hint) {
        return streamVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamVector(String key, int clientId) {
        return streamVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint) {
        return streamVector(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Dumps or streams all entries from an unordered Map container.
     */
    CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Map<Payload, Payload>> streamMap(String key) {
        return streamMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<Payload, Payload>> streamMap(String key, KeyHintData hint) {
        return streamMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches a slice (range) of elements from an unordered container based on position indexes.
     */
    CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                   KeyHintData hint,
                                                                   ContainerType containerType,
                                                                   int start,
                                                                   int end,
                                                                   int clientId,
                                                                   Duration timeout);

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end,
                                                                           int clientId) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             null,
                                             containerType,
                                             start,
                                             end,
                                             clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(key,
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    /**
     * Deletes matching values/keys from the given container type.
     *
     * @return count of successfully removed elements.
     */
    CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                   KeyHintData hint,
                                                   ContainerType type,
                                                   List<Payload> values,
                                                   List<Payload> keys,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(String key, ContainerType type, List<Payload> values) {
        return removeFromContainer(serializeKey(key),
                                   null,
                                   type,
                                   values,
                                   Collections.emptyList(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                           KeyHintData hint,
                                                           ContainerType type,
                                                           List<Payload> values) {
        return removeFromContainer(key,
                                   hint,
                                   type,
                                   values,
                                   Collections.emptyList(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    // =========================================================================
    // ORDERED CONTAINERS (OrderedPayload)
    // =========================================================================

    /**
     * Creates an OrderedSet container containing weight/score-ranked {@link OrderedPayload} elements.
     */
    CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                    List<OrderedPayload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedSet(String key, List<OrderedPayload> initialValue) {
        return createOrderedSet(serializeKey(key),
                                initialValue == null
                                ? Collections.emptyList()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedPayload> initialValue) {
        return createOrderedSet(key,
                                initialValue == null
                                ? Collections.emptyList()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    /**
     * Creates an OrderedMap container where keys are instance of {@link OrderedPayload}.
     */
    CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                    Map<OrderedPayload, Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<KeyHintData> createOrderedMap(String key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(serializeKey(key),
                                initialValue == null
                                ? Collections.emptyMap()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Payload> initialValue) {
        return createOrderedMap(key,
                                initialValue == null
                                ? Collections.emptyMap()
                                : initialValue,
                                getDefaultTtl(),
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    /**
     * Streams or dumps all entries stored in an OrderedMap.
     */
    CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key,
                                                                     KeyHintData hint,
                                                                     int clientId,
                                                                     Duration timeout);

    default CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(String key) {
        return streamOrderedMap(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(String key, KeyHintData hint) {
        return streamOrderedMap(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches a sub-range of elements from an {@link OrderedSet} filtered by score/weight boundaries.
     *
     * @param startWeight lower bound weight limit.
     * @param endWeight   upper bound weight limit.
     * @param reverse     {@code true} for descending ordering, {@code false} for ascending.
     */
    CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key,
                                                                           KeyHintData hint,
                                                                           long startWeight,
                                                                           long endWeight,
                                                                           boolean reverse,
                                                                           int clientId,
                                                                           Duration timeout);

    default CompletableFuture<List<OrderedPayload>> streamElementInRangeOrdered(String key,
                                                                                ContainerType containerType,
                                                                                long startWeight,
                                                                                long endWeight) {
        return streamElementInRangeOrderedSet(serializeKey(key),
                                              null,
                                              startWeight,
                                              endWeight,
                                              false,
                                              getDefaultClientId(),
                                              getDefaultTimeout());
    }

    /**
     * Adds weighted elements to an {@link OrderedSet}.
     */
    CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedPayload> data,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementWithWeight(String key, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    // =========================================================================
    // ATOMIC & LOCK OPERATIONS
    // =========================================================================

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
     * Removes the tail element of a sequence without returning the deleted element.
     */
    CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHintData hint) {
        return removeTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Removes the head element of a sequence without returning the deleted element.
     */
    CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, KeyHintData hint) {
        return removeHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Removes a range of elements by position indexes.
     */
    CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                       KeyHintData hint,
                                                       int pos,
                                                       int endPos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key,
                                                               KeyHintData hint,
                                                               int pos,
                                                               int endPos,
                                                               int clientId) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically creates a 64-bit primitive scalar value.
     */
    CompletableFuture<KeyHintData> atomicCreate(byte[] key,
                                                KeyHintData hint,
                                                long value,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value) {
        return atomicCreate(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, KeyHintData hint, long value) {
        return atomicCreate(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, long value) {
        return atomicCreate(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value, int clientId) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically overwrites a 64-bit scalar counter/value.
     */
    CompletableFuture<KeyHintData> atomicStore(byte[] key,
                                               KeyHintData hint,
                                               long value,
                                               Duration ttl,
                                               int clientId,
                                               Duration timeout);

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value) {
        return atomicStore(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData hint, long value) {
        return atomicStore(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, long value) {
        return atomicStore(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData keyHint, long value, int clientId) {
        return atomicStore(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically sets a new 64-bit value and returns the old value.
     */
    CompletableFuture<Long> atomicExchange(byte[] key,
                                           KeyHintData hint,
                                           long value,
                                           Duration ttl,
                                           int clientId,
                                           Duration timeout);

    default CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value) {
        return atomicExchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, KeyHintData hint, long value) {
        return atomicExchange(serializeKey(key),
                              hint,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, long value) {
        return atomicExchange(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically increments a 64-bit scalar value by a given delta.
     */
    CompletableFuture<Long> atomicAdd(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta) {
        return atomicAdd(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, KeyHintData hint, long delta) {
        return atomicAdd(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, long delta) {
        return atomicAdd(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically decrements a 64-bit scalar value by a given delta.
     */
    CompletableFuture<Long> atomicSub(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta) {
        return atomicSub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, KeyHintData hint, long delta) {
        return atomicSub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise AND mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicAnd(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask) {
        return atomicAnd(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, KeyHintData hint, long mask) {
        return atomicAnd(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise OR mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicOr(byte[] key,
                                     KeyHintData hint,
                                     long mask,
                                     Duration ttl,
                                     int clientId,
                                     Duration timeout);

    default CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask) {
        return atomicOr(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, KeyHintData keyHint, long mask) {
        return atomicOr(serializeKey(key), keyHint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise XOR mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicXor(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask) {
        return atomicXor(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, KeyHintData hint, long mask) {
        return atomicXor(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Performs an atomic Compare-And-Swap (CAS) operation on a primitive 64-bit scalar.
     *
     * @param expectedValue expected existing scalar value.
     * @param newValue      new target value to apply if match succeeds.
     * @return resulting {@link AtomicCasRes} indicating success/failure and current value.
     */
    CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                        KeyHintData hint,
                                                        long expectedValue,
                                                        long newValue,
                                                        Duration ttl,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                                KeyHintData keyHint,
                                                                long expectedValue,
                                                                long newValue) {
        return atomicCompareAndSet(key,
                                   keyHint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key,
                                                                KeyHintData keyHint,
                                                                long expectedValue,
                                                                long newValue) {
        return atomicCompareAndSet(serializeKey(key),
                                   keyHint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key,
                                                                KeyHintData hint,
                                                                long expectedValue,
                                                                long newValue,
                                                                int clientId) {
        return atomicCompareAndSet(serializeKey(key),
                                   hint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   clientId,
                                   getDefaultTimeout());
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
     * Loads current value of an atomic scalar.
     */
    CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoad(String key) {
        return atomicLoad(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoad(String key, int clientId) {
        return atomicLoad(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(String key, KeyHintData hint) {
        return atomicLoad(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint) {
        return atomicLoad(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Reads current value of an atomic scalar and immediately removes it.
     */
    CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoadAndDelete(String key) {
        return atomicLoadAndDelete(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, int clientId) {
        return atomicLoadAndDelete(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, KeyHintData hint) {
        return atomicLoadAndDelete(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint) {
        return atomicLoadAndDelete(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> getContainerValue(byte[] key,
                                                KeyHintData hint,
                                                byte[] elementKey,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<byte[]> getContainerValue(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getContainerValue(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key,
                                                         KeyHintData hint,
                                                         byte[] elementKey,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getAndRemoveContainerValue(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> containsContainerKey(byte[] key,
                                                    KeyHintData hint,
                                                    byte[] elementKey,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Boolean> containsContainerKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return containsContainerKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> updateContainerValue(byte[] key,
                                                   KeyHintData hint,
                                                   byte[] elementKey,
                                                   byte[] value,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<byte[]> updateContainerValue(byte[] key,
                                                            KeyHintData hint,
                                                            byte[] elementKey,
                                                            byte[] value) {
        return updateContainerValue(key, hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                   KeyHintData hint,
                                                   byte[] elementKey,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, byte[] elementKey) {
        return removeFromContainer(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Gracefully shuts down client connections and releases network resources.
     */
    void shutdown();
}
package com.hurricache.client.intf;

import com.hurricache.client.KeyUtils;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Client interface for interacting with the HurriCache distributed cache system.
 * Provides asynchronous operations for key-value manipulation, collection structures
 * (Queues, Lists, Vectors), object locking, and atomic numerical/bitwise operations.
 *
 * <p>Most methods provide asynchronous execution returning a {@link CompletableFuture}.
 * Overloaded methods exist to provide convenient defaults for {@code clientId},
 * {@code timeout}, and {@code KeyHint} serialization.
 */
public interface HurriCacheClientInterface {

    /**
     * Gets the default client identifier used for tracking requests.
     *
     * @return the default client ID.
     */
    int getDefaultClientId();

    /**
     * Gets the default timeout duration for operations.
     *
     * @return the default operation timeout {@link Duration}.
     */
    Duration getDefaultTimeout();

    /**
     * Gets the default Time-To-Live (TTL) duration for newly created keys.
     *
     * @return the default TTL duration, or {@code null} if no default is specified.
     */
    default Duration getDefaultTtl() {
        return null;
    }

    /**
     * Gets the connection target address of the HurriCache server/cluster.
     *
     * @return the target connection string.
     */
    String getTarget();

    /**
     * Serializes a string key into a byte array using UTF-8 encoding.
     *
     * @param key the string key to serialize.
     * @return the byte array representation of the key.
     */
    default byte[] serializeKey(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generates a routing or partitioning hint based on the weak hash of the key.
     *
     * @param key the byte array representing the cache key.
     * @return a {@link KeyHint} instance wrapping the generated hash.
     */
    default KeyHint getKeyHint(byte[] key) {
        return KeyHint.newBuilder().setWeekHash(KeyUtils.weakHash(key, key.length, 0)).build();
    }

    default CompletableFuture<Boolean> setTtl(String key, KeyHint hint, long ttl) {
        return setTtl(key, hint, ttl, getDefaultClientId());
    }

    default CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId) {
        return setTtl(key, hint, ttl, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> setTtl(String key, KeyHint hint, long ttl, int clientId) {
        return setTtl(serializeKey(key), hint, ttl, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl) {
        return setTtl(key, hint, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Explicitly sets or updates the Time-To-Live (TTL) expiration for a given key.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param ttl      the expiration duration in seconds or milliseconds depending on implementation.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if the TTL was successfully applied, otherwise {@code false}.
     */
    CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout);

    /**
     * Retrieves the remaining Time-To-Live (TTL) expiration of a given key.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the remaining TTL value, or a negative value if it does not expire or exist.
     */
    CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Long> getTtl(String key) {
        return getTtl(key, getDefaultClientId());
    }

    default CompletableFuture<Long> getTtl(String key, KeyHint hint) {
        return getTtl(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(byte[] key, KeyHint hint) {
        return getTtl(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(String key, int clientId) {
        return getTtl(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Atomically retrieves the payload value of a key and deletes it from the cache.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the byte array value before deletion, or {@code null} if missing.
     */
    CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint, int clientId) {
        return getAndDeleteValue(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    /**
     * Creates a new key-value pair in the cache.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param value    the byte array data payload to store.
     * @param ttl      the expiration duration for the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> createKeyValue(byte[] key, KeyHint hint, byte[] value, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId, Duration timeout) {
        return createKeyValue(key, getKeyHint(key), value, getDefaultTtl(), clientId, timeout);
    }

    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value, int clientId) {
        return createKeyValue(serializeKey(key), value, clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId) {
        return createKeyValue(key, value, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves the byte payload value corresponding to the specified key.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the byte array value, or {@code null} if not found.
     */
    CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getValue(String key) {
        return getValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getValue(byte[] key) {
        return getValue(key, getKeyHint(key), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHint hint) {
        return getValue(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint) {
        return getValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint hint, byte[] value) {
        return updateKeyValue(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, int clientId) {
        return getValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(byte[] key, KeyHint keyhint, int clientId) {
        return getValue(key, keyhint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHint keyhint, int clientId) {
        return getValue(serializeKey(key), keyhint, clientId, getDefaultTimeout());
    }

    /**
     * Updates an existing key-value record with a new value payload and refreshes its TTL.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param value    the new byte array payload to set.
     * @param ttl      the updated expiration duration.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the old byte array value prior to modification.
     */
    CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint hint, byte[] value, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value) {
        return updateKeyValue(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint keyHint, byte[] value, int clientID) {
        return updateKeyValue(key, keyHint, value, getDefaultTtl(), clientID, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHint keyHint, byte[] value, int clientID) {
        return updateKeyValue(serializeKey(key), keyHint, value, getDefaultTtl(), clientID, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHint hint, byte[] value) {
        return updateKeyValue(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value, int clientId) {
        return updateKeyValue(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Checks whether the specified key exists in the cache.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if the key exists, otherwise {@code false}.
     */
    CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> existKey(String key) {
        return existKey(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key) {
        return existKey(key, getKeyHint(key), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, KeyHint hint) {
        return existKey(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint) {
        return existKey(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, int clientId) {
        return existKey(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Deletes and removes a record from the cache by its key.
     *
     * @param key      the byte array key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if the element was removed successfully, otherwise {@code false}.
     */
    CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> remove(String key) {
        return remove(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHint hint) {
        return remove(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, int clientId) {
        return remove(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(byte[] key, KeyHint hint) {
        return remove(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientid) {
        return remove(key, hint, clientid, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHint hint, int clientid) {
        return remove(serializeKey(key), hint, clientid, getDefaultTimeout());
    }

    /**
     * Initializes a FIFO Queue structure under the specified key.
     *
     * @param key          the byte array key representing the queue.
     * @param initialValue initial list of byte payloads to enqueue during creation.
     * @param ttl          the structure expiration timeout.
     * @param clientId     the identifier of the invoking client.
     * @param timeout      the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue, Duration ttl) {
        return createQueue(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createQueue(String key) {
        return createQueue(serializeKey(key), Collections.emptyList(), getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createQueue(String key, List<byte[]> initialValue) {
        return createQueue(serializeKey(key), initialValue == null ? Collections.emptyList() : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue) {
        return createQueue(key, initialValue == null ? Collections.emptyList() : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Initializes a sequential List structure under the specified key.
     *
     * @param key          the byte array key representing the list.
     * @param initialValue initial collection of items to seed the list with.
     * @param ttl          the structure expiration timeout.
     * @param clientId     the identifier of the invoking client.
     * @param timeout      the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue, Duration ttl) {
        return createList(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createList(String key) {
        return createList(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue) {
        return createList(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue) {
        return createList(key, initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue, int clientId) {
        return createList(serializeKey(key), initialValue == null ? Collections.emptyList() : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Initializes an indexable Vector structure under the specified key.
     *
     * @param key          the byte array key representing the vector.
     * @param initialValue initial collection of items to seed the vector with.
     * @param ttl          the structure expiration timeout.
     * @param clientId     the identifier of the invoking client.
     * @param timeout      the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue, Duration ttl) {
        return createVector(key, initialValue, ttl, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createVector(String key) {
        return createVector(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue) {
        return createVector(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue) {
        return createVector(key, initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue, int clientId) {
        return createVector(serializeKey(key), initialValue == null ? Collections.emptyList() : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Pops and removes the head (front) item from a structure (e.g., Queue or List).
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the extracted front byte array element.
     */
    CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveFront(String key) {
        return getAndRemoveFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, KeyHint hint) {
        return getAndRemoveFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, int clientId) {
        return getAndRemoveFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Looks at the front element of a structure without removing it.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the front byte array element.
     */
    CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getFront(String key) {
        return getFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(byte[] key) {
        return getFront(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, KeyHint hint) {
        return getFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, KeyHint hint, int clientId) {
        return getFront(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, int clientId) {
        return getFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint) {
        return getFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Appends an array of data elements to the end (tail) of the target collection structure.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param data     the sequence of byte arrays to insert.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if successful, otherwise {@code false}.
     */
    CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHint hint, List<byte[]> data, int clientId, Duration timeout);

    default CompletableFuture<Boolean> addElementToTail(String key, KeyHint hint, List<byte[]> data) {
        return addElementToTail(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(String key, KeyHint hint, List<byte[]> data, int clientId) {
        return addElementToTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHint hint, List<byte[]> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches a single entry located at a specific index location within a List or Vector structure.
     *
     * @param key      the target collection key.
     * @param hint     the key hint for routing optimization.
     * @param pos      the positional index (0-based) to request.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the element's byte array, or {@code null} if out of bounds.
     */
    CompletableFuture<byte[]> getElementAtPosition(byte[] key, KeyHint hint, int pos, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getElementAtPosition(String key, KeyHint hint, int pos) {
        return getElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams or downloads the entire contents of a cached List structure.
     *
     * @param key      the list key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping a {@link List} containing all elements.
     */
    CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamList(String key) {
        return streamList(key, getDefaultClientId());
    }

    default CompletableFuture<List<byte[]>> streamList(String key, int clientId) {
        return streamList(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamList(String key, KeyHint hint) {
        return streamList(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint) {
        return streamList(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Acquires a lock on a specific key/object to orchestrate concurrency control.
     *
     * @param key      the object key to lock.
     * @param hint     the key hint for routing optimization.
     * @param type     the type of isolation barrier requested (e.g., Read, Write).
     * @param clientId the identifier of the client holding the lock.
     * @param duration the validity/lease duration of the lock.
     * @param timeout  the acquisition timeout boundary.
     * @return a {@link CompletableFuture} returning the acquisition status via {@link LockStatus}.
     */
    CompletableFuture<LockStatus> lockObject(byte[] key, KeyHint hint, LockType type, int clientId, Duration duration, Duration timeout);

    default CompletableFuture<LockStatus> lockObject(byte[] key, KeyHint hint, LockType type, Duration duration) {
        return lockObject(key, hint, type, getDefaultClientId(), duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, KeyHint hint, LockType type, int clientId, Duration duration) {
        return lockObject(serializeKey(key), hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(byte[] key, KeyHint hint, LockType type, int clientId, Duration duration) {
        return lockObject(key, hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, int clientId, Duration duration) {
        return lockObject(serializeKey(key), null, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, Duration duration) {
        return lockObject(serializeKey(key), null, type, getDefaultClientId(), duration, getDefaultTimeout());
    }

    /**
     * Explicitly releases a locked object resource.
     *
     * @param key      the object key to unlock.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the client releasing the lock.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the resulting execution {@link LockStatus}.
     */
    CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<LockStatus> unlockObject(String key, KeyHint hint, int clientId) {
        return unlockObject(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint) {
        return unlockObject(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientid) {
        return unlockObject(key, hint, clientid, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key, int clientId) {
        return unlockObject(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key) {
        return unlockObject(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams elements belonging to a specific sub-range subset out of an array or sequential structure.
     *
     * @param key      the unique identifier for the structure.
     * @param hint     the key hint for routing optimization.
     * @param isArray  flag stating if the structure should be processed as a standard primitive array.
     * @param start    the index boundary start position (inclusive).
     * @param end      the index boundary stopping position (exclusive).
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} containing the matching item list segment payload.
     */
    CompletableFuture<List<byte[]>> streamElementInRange(byte[] key, KeyHint hint, boolean isArray, int start, int end, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamElementInRange(String key, KeyHint hint, boolean isArray, int start, int end) {
        return streamElementInRange(serializeKey(key), hint, isArray, start, end, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamElementInRange(String key, boolean isArray, int start, int end, int clientId) {
        return streamElementInRange(serializeKey(key), null, isArray, start, end, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamElementInRange(byte[] key, KeyHint hint, boolean isArray, int start, int end) {
        return streamElementInRange(key, hint, isArray, start, end, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Streams or downloads all entries contained inside a Vector structure.
     *
     * @param key      the vector structure key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping a list containing the vector's entries.
     */
    CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamVector(String key) {
        return streamVector(key, null);
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, KeyHint hint) {
        return streamVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, int clientId) {
        return streamVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint) {
        return streamVector(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically retrieves and purges an item located at an exact position coordinate inside a structural collection.
     *
     * @param key      the targeted collection key.
     * @param hint     the key hint for routing optimization.
     * @param pos      the zero-based index to extract.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the extracted item payload.
     */
    CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key, KeyHint hint, int pos, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, KeyHint hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, KeyHint hint, int pos, int clientId) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Prepends an array of elements directly to the head (front) of a targeted structure.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param data     the collection of payload data pieces to push forward.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if successful, otherwise {@code false}.
     */
    CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHint hint, List<byte[]> data, int clientId, Duration timeout);

    default CompletableFuture<Boolean> addElementToHead(String key, List<byte[]> data) {
        return addElementToHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(String key, KeyHint hint, List<byte[]> data) {
        return addElementToHead(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHint keyhint, List<byte[]> data) {
        return addElementToHead(key, keyhint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts data records directly into a specific index location, shifting succeeding indices rightward.
     *
     * @param key      the structural key entity.
     * @param hint     the key hint for routing optimization.
     * @param data     the sequence of item byte lists to embed.
     * @param pos      the target index coordinate slot.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if the entry was added safely, otherwise {@code false}.
     */
    CompletableFuture<Boolean> addElementToPosition(byte[] key, KeyHint hint, List<byte[]> data, int pos, int clientId, Duration timeout);

    default CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos) {
        byte[] key1 = serializeKey(key);
        return addElementToPosition(key1, getKeyHint(key1), data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPosition(String key, KeyHint hint, List<byte[]> data, int pos) {
        return addElementToPosition(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos, int clientId) {
        byte[] key1 = serializeKey(key);
        return addElementToPosition(key1, getKeyHint(key1), data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPosition(byte[] key, KeyHint hint, List<byte[]> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Trims or drops the final ending item (tail) from the structured type.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if an item was removed, otherwise {@code false}.
     */
    CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHint hint) {
        return removeTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Trims or drops the leading item (head) from the structured type.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if an item was removed, otherwise {@code false}.
     */
    CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, KeyHint hint) {
        return removeHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Deletes a range of elements residing between index limits.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param pos      the initial starting offset position boundary (inclusive).
     * @param endPos   the final ending index boundary limit (exclusive).
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping {@code true} if the subset region was emptied, otherwise {@code false}.
     */
    CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHint hint, int pos, int endPos, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHint hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHint hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHint hint, int pos, int endPos, int clientId) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHint hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Shuts down the client connection pool and releases allocated communication pipeline resources.
     */
    void shutdown();

    /**
     * Looks at the head element of a structure without removing it.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the head byte array element.
     */
    CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getHead(String key) {
        return getHead(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getHead(String key, KeyHint hint) {
        return getHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Looks at the tail element of a structure without removing it.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the tail byte array element.
     */
    CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getTail(String key) {
        return getTail(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getTail(String key, KeyHint hint) {
        return getTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Pops and removes the tail (rear) item from a structure.
     *
     * @param key      the collection key.
     * @param hint     the key hint for routing optimization.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the extracted tail byte array element.
     */
    CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveTail(String key) {
        return getTail(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndRemoveTail(String key, KeyHint hint) {
        return getTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Null-safe helper that returns a fallback default KeyHint if the provided hint reference is null.
     *
     * @param key  the cache byte array key.
     * @param hint a nullable user-provided KeyHint.
     * @return the provided hint if non-null, or a newly generated default KeyHint.
     */
    default KeyHint getKeyHint(byte[] key, KeyHint hint) {
        return hint == null ? getKeyHint(key) : hint;
    }

    // =========================================================================
    // ATOMIC OPERATIONS (USING LONG FOR COUNTERS / MASKS)
    // =========================================================================

    /**
     * Initialized an atomic counter or 64-bit numerical field under the specified key.
     *
     * @param key      the counter entry key.
     * @param hint     the key hint for routing optimization.
     * @param value    the primitive long scalar integer initializer.
     * @param ttl      the expiration lifetime limit.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> atomicCreate(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> atomicCreate(byte[] key, KeyHint hint, long value) {
        return atomicCreate(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicCreate(String key, KeyHint hint, long value) {
        return atomicCreate(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicCreate(String key, long value) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicCreate(byte[] key, long value) {
        return atomicCreate(key, getKeyHint(key), value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicCreate(String key, long value, int clientId) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Overwrites or stores an atomic numerical value to a given long integer value.
     *
     * @param key      the entry key.
     * @param hint     the key hint for routing optimization.
     * @param value    the long integer value to assign.
     * @param ttl      the entry lifetime limit.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the confirmed {@link KeyHint}.
     */
    CompletableFuture<KeyHint> atomicStore(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> atomicStore(byte[] key, KeyHint hint, long value) {
        return atomicStore(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicStore(String key, KeyHint hint, long value) {
        return atomicStore(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicStore(byte[] key, long value) {
        return atomicStore(key, getKeyHint(key), value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> atomicStore(String key, KeyHint keyHint, long value, int clientId) {
        return atomicStore(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically exchanges a numerical field value, replacing it with a new one and returning the prior state value.
     *
     * @param key      the unique entry key.
     * @param hint     the key hint for routing optimization.
     * @param value    the substitution replacement value.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the old numerical value before substitution.
     */
    CompletableFuture<Long> atomicExchange(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicExchange(byte[] key, KeyHint hint, long value) {
        return atomicExchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, KeyHint hint, long value) {
        return atomicExchange(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, long value) {
        return atomicExchange(key, getKeyHint(key), value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Performs an atomic addition operation on a remote long counter field.
     *
     * @param key      the counter entry key.
     * @param hint     the key hint for routing optimization.
     * @param delta    the value amount increment offset to apply.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the freshly updated value post-addition.
     */
    CompletableFuture<Long> atomicAdd(byte[] key, KeyHint hint, long delta, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicAdd(byte[] key, KeyHint hint, long delta) {
        return atomicAdd(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, KeyHint hint, long delta) {
        return atomicAdd(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, long delta) {
        return atomicAdd(key, getKeyHint(key), delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Performs an atomic subtraction operation on a remote long counter field.
     *
     * @param key      the counter entry key.
     * @param hint     the key hint for routing optimization.
     * @param delta    the value amount decrement offset to apply.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the freshly updated value post-subtraction.
     */
    CompletableFuture<Long> atomicSub(byte[] key, KeyHint hint, long delta, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicSub(byte[] key, KeyHint hint, long delta) {
        return atomicSub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, KeyHint hint, long delta) {
        return atomicSub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Computes an atomic bitwise AND operation across the given 64-bit mask against a specific value entry.
     *
     * @param key      the entry key.
     * @param hint     the key hint for routing optimization.
     * @param mask     the bitwise mask parameter.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the newly computed bitwise value.
     */
    CompletableFuture<Long> atomicAnd(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicAnd(byte[] key, KeyHint hint, long mask) {
        return atomicAnd(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, KeyHint hint, long mask) {
        return atomicAnd(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Computes an atomic bitwise OR operation across the given 64-bit mask against a specific value entry.
     *
     * @param key      the entry key.
     * @param hint     the key hint for routing optimization.
     * @param mask     the bitwise mask parameter.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the newly computed bitwise value.
     */
    CompletableFuture<Long> atomicOr(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicOr(byte[] key, KeyHint hint, long mask) {
        return atomicOr(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, KeyHint keyHint, long mask) {
        return atomicOr(serializeKey(key), keyHint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Computes an atomic bitwise XOR (exclusive or) operation across the given 64-bit mask against a specific value entry.
     *
     * @param key      the entry key.
     * @param hint     the key hint for routing optimization.
     * @param mask     the bitwise mask parameter.
     * @param ttl      the lifetime limit of the entry.
     * @param clientId the identifier of the invoking client.
     * @param timeout  the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping the newly computed bitwise value.
     */
    CompletableFuture<Long> atomicXor(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicXor(byte[] key, KeyHint hint, long mask) {
        return atomicXor(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, KeyHint hint, long mask) {
        return atomicXor(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Executes an atomic Compare-And-Set (CAS) conditional value assignment state adjustment.
     *
     * @param key           the entry key.
     * @param hint          the key hint for routing optimization.
     * @param expectedValue the prerequisite value expected to match within cache storage state.
     * @param newValue      the target assigned value written if the prerequisite match succeeds.
     * @param ttl           the expiration lifetime limit.
     * @param clientId      the identifier of the invoking client.
     * @param timeout       the operation execution timeout.
     * @return a {@link CompletableFuture} wrapping an {@link AtomicCasRes} envelope depicting transaction results.
     */
    CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHint hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHint keyHint, long expectedValue, long newValue) {
        return atomicCompareAndSet(key, keyHint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, KeyHint keyHint, long expectedValue, long newValue) {
        return atomicCompareAndSet(serializeKey(key), keyHint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, KeyHint hint, long expectedValue, long newValue, int clientId) {
        return atomicCompareAndSet(serializeKey(key), hint, expectedValue, newValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }
}
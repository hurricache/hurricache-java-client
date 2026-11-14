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
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint, List<Variant> initialValue,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout);

    /**
     * Creates a Set container using a string key.
     *
     * @param key          target container key as a string (encoded as UTF-8).
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param ttl          container expiration duration.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(String key, KeyHintData keyHint,
                                                     List<Variant> initialValue,
                                                     Duration ttl, int clientId, Duration timeout) {
        return createSet(serializeKey(key), keyHint, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, ttl, clientId, timeout);
    }

    /**
     * Creates a Set container using a string key, the default TTL and the default timeout.
     *
     * @param key          target container key as a string (encoded as UTF-8).
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param clientId     identifier of the issuing client.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(String key, KeyHintData keyHint,
                                                     List<Variant> initialValue, int clientId) {
        return createSet(serializeKey(key), keyHint, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Creates a Set container using a string key and default client settings.
     *
     * @param key          target container key as a string (encoded as UTF-8).
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(String key, KeyHintData keyHint,
                                                     List<Variant> initialValue) {
        return createSet(serializeKey(key), keyHint, initialValue == null
                                                     ? Collections.emptyList()
                                                     : initialValue,
                         getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Creates a Set container using a string key and default client settings.
     *
     * @param key          target container key as a string (encoded as UTF-8).
     * @param initialValue initial sequence of {@link Variant} elements.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(String key, List<Variant> initialValue) {
        return createSet(serializeKey(key), null, initialValue == null
                                                  ? Collections.emptyList()
                                                  : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Creates a Set container using default client settings.
     *
     * @param key          target container key in byte array form.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(byte[] key, List<Variant> initialValue) {
        return createSet(key, null, initialValue == null
                                    ? Collections.emptyList()
                                    : initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Creates a Set container using the default TTL and the default timeout.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @param clientId     identifier of the issuing client.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint,
                                                     List<Variant> initialValue, int clientId) {
        return createSet(key, keyHint, initialValue == null
                                       ? Collections.emptyList()
                                       : initialValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Creates a Set container using default client settings.
     *
     * @param key          target container key in byte array form.
     * @param keyHint      optional key routing hint.
     * @param initialValue initial sequence of {@link Variant} elements.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    default CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint,
                                                     List<Variant> initialValue) {
        return createSet(key, keyHint, initialValue == null
                                       ? Collections.emptyList()
                                       : initialValue,
                         getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container (e.g., Set, HashSet).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link Variant} elements to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    CompletableFuture<Integer> addElementUnordered(byte[] key,
                                                   KeyHintData hint,
                                                   List<Variant> data,
                                                   int clientId,
                                                   Duration timeout);

    /**
     * Adds elements to an unordered container using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param data     list of {@link Variant} elements to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint,
                                                           List<Variant> data,
                                                           int clientId, Duration timeout) {
        return addElementUnordered(serializeKey(key), hint, data, clientId, timeout);
    }

    /**
     * Adds elements to an unordered container using a string key and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param data list of {@link Variant} elements to add.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(String key, List<Variant> data) {
        return addElementUnordered(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param data list of {@link Variant} elements to add.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(byte[] key, List<Variant> data) {
        return addElementUnordered(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container using a string key and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param hint optional key routing hint.
     * @param data list of {@link Variant} elements to add.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint, List<Variant> data) {
        return addElementUnordered(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param hint optional key routing hint.
     * @param data list of {@link Variant} elements to add.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Variant> data) {
        return addElementUnordered(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container using a string key and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param data     list of {@link Variant} elements to add.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(String key, KeyHintData hint, List<Variant> data,
                                                           int clientId) {
        return addElementUnordered(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Adds elements to an unordered container using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link Variant} elements to add.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of unique elements added.
     */
    default CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Variant> data,
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
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    CompletableFuture<List<Variant>> getSet(byte[] key, KeyHintData hint, int clientId,
                                            Duration timeout);

    /**
     * Fetches all elements contained in an unordered Set container using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(String key, KeyHintData hint, int clientId,
                                                    Duration timeout) {
        return getSet(serializeKey(key), hint, clientId, timeout);
    }

    /**
     * Fetches all elements contained in an unordered Set container using a string key
     * and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param hint optional key routing hint.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(String key, KeyHintData hint) {
        return getSet(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using a string key
     * and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(String key, KeyHintData hint, int clientId) {
        return getSet(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(byte[] key, KeyHintData hint, int clientId) {
        return getSet(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using a string key
     * and default client settings.
     *
     * @param key target container key as a string (encoded as UTF-8).
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(String key) {
        return getSet(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using default client settings.
     *
     * @param key target container key in byte array form.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(byte[] key) {
        return getSet(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using a string key
     * and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(String key, int clientId) {
        return getSet(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Fetches all elements contained in an unordered Set container using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing list of all {@link Variant} elements.
     */
    default CompletableFuture<List<Variant>> getSet(byte[] key, int clientId) {
        return getSet(key, null, clientId, getDefaultTimeout());
    }
}

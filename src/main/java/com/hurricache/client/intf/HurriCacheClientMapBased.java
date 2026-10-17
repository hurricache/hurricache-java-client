package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientMapBased extends HurriCacheClientInterfaceCommon{
    /**
     * Retrieves a value associated with an element key from a container.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing the value as Variant.
     */
    CompletableFuture<Variant> getInContainer(byte[] key,
                                              KeyHintData hint,
                                              byte[] elementKey,
                                              int clientId,
                                              Duration timeout);

    default CompletableFuture<Variant> getInContainer(String key, KeyHintData hint, byte[] elementKey,
                                                      Duration ttl, int clientId, Duration timeout) {
        return getInContainer(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Variant> getInContainer(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getInContainer(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getInContainer(String key, KeyHintData hint, byte[] elementKey) {
        return getInContainer(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getInContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getInContainer(key, hint, elementKey, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getInContainer(String key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getInContainer(serializeKey(key), hint, elementKey, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves and removes a value associated with an element key from a container.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing the deleted value as Variant.
     */
    CompletableFuture<Variant> popInContainer(byte[] key,
                                              KeyHintData hint,
                                              byte[] elementKey,
                                              int clientId,
                                              Duration timeout);

    default CompletableFuture<Variant> popInContainer(String key, KeyHintData hint, byte[] elementKey,
                                                      int clientId, Duration timeout) {
        return popInContainer(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Variant> popInContainer(byte[] key, KeyHintData hint, byte[] elementKey) {
        return popInContainer(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popInContainer(String key, KeyHintData hint, byte[] elementKey) {
        return popInContainer(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Checks if a container contains the specified element key.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} returning true if key exists.
     */
    CompletableFuture<Boolean> containsKey(byte[] key,
                                           KeyHintData hint,
                                           byte[] elementKey,
                                           int clientId,
                                           Duration timeout);

    default CompletableFuture<Boolean> containsKey(String key, KeyHintData hint, byte[] elementKey,
                                                   int clientId, Duration timeout) {
        return containsKey(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return containsKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsKey(String key, KeyHintData hint, byte[] elementKey) {
        return containsKey(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Updates the value associated with an element key in a container.
     * The value can be bytes, int, or double depending on the {@link Variant} type.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param value       new value (Variant: bytes, int, or double).
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing the previous value as Variant.
     */
    CompletableFuture<Variant> update(byte[] key,
                                      KeyHintData hint,
                                      byte[] elementKey,
                                      Variant value,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Variant> update(String key, KeyHintData hint, byte[] elementKey,
                                              Variant value, int clientId, Duration timeout) {
        return update(serializeKey(key), hint, elementKey, value, clientId, timeout);
    }

    default CompletableFuture<Variant> update(byte[] key, KeyHintData hint, byte[] elementKey,
                                              Variant value) {
        return update(key, hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> update(String key, KeyHintData hint, byte[] elementKey,
                                              Variant value) {
        return update(serializeKey(key), hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Removes an element from a container by its key.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing count of removed elements.
     */
    CompletableFuture<Integer> remove(byte[] key,
                                      KeyHintData hint,
                                      byte[] elementKey,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Integer> remove(String key, KeyHintData hint, byte[] elementKey,
                                              int clientId, Duration timeout) {
        return remove(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Integer> remove(byte[] key, KeyHintData hint, byte[] elementKey) {
        return remove(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> remove(String key, KeyHintData hint, byte[] elementKey) {
        return remove(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to a HashMap container.
     * The values can be bytes, int, or double depending on the {@link Variant} type.
     *
     * @param key             target container key in byte array form.
     * @param hint            optional key routing hint.
     * @param container_keys  list of payload keys.
     * @param container_values list of variant values.
     * @param clientId        identifier of the issuing client.
     * @param timeout         execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> putAll(byte[] key,
                                      KeyHintData hint,
                                      List<Payload> container_keys,
                                      List<Variant> container_values,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Integer> putAll(String key, KeyHintData hint,
                                              List<Payload> container_keys,
                                              List<Variant> container_values,
                                              int clientId, Duration timeout) {
        return putAll(serializeKey(key), hint, container_keys, container_values, clientId, timeout);
    }

    default CompletableFuture<Integer> putAll(byte[] key, KeyHintData hint,
                                              List<Payload> container_keys,
                                              List<Variant> container_values) {
        return putAll(key, hint, container_keys, container_values,
                      getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> putAll(String key, KeyHintData hint,
                                              List<Payload> container_keys,
                                              List<Variant> container_values) {
        return putAll(serializeKey(key), hint, container_keys, container_values,
                      getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> putAll(String key, KeyHintData hint,
                                              List<Payload> container_keys,
                                              List<Variant> container_values, int clientId) {
        return putAll(serializeKey(key), hint, container_keys, container_values,
                      clientId, getDefaultTimeout());
    }


    default CompletableFuture<Integer> putAll(byte[] key, List<Payload> container_keys,
                                              List<Variant> container_values) {
        return putAll(key, null, container_keys, container_values,
                      getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> putAll(byte[] key, List<Payload> container_keys,
                                              List<Variant> container_values, int clientId) {
        return putAll(key, null, container_keys, container_values,
                      clientId, getDefaultTimeout());
    }


    default CompletableFuture<Integer> putAll(String key, List<Payload> container_keys,
                                              List<Variant> container_values) {
        return putAll(serializeKey(key), null, container_keys, container_values,
                      getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Integer> putAll(String key, List<Payload> container_keys,
                                              List<Variant> container_values, int clientId) {
        return putAll(serializeKey(key), null, container_keys, container_values,
                      clientId, getDefaultTimeout());
    }

    /**
     * Deletes matching values/keys from the given container type.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param keys     list of keys to match.
     * @param values   list of values to match.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return count of successfully removed elements.
     */
    CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key,
                                                                KeyHintData hint, List<Payload> keys,
                                                                List<Variant> values,
                                                                int clientId,
                                                                Duration timeout);

    default CompletableFuture<Integer> removeFromContainerByKeyAndValue(String key, KeyHintData hint, ContainerType type,
                                                                        List<Payload> keys, List<Variant> values,
                                                                        int clientId, Duration timeout) {
        return removeFromContainerByKeyAndValue(serializeKey(key), hint, keys, values, clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainerByKeyAndValue(String key, KeyHintData hint, ContainerType type,
                                                                        List<Payload> keys, List<Variant> values) {
        return removeFromContainerByKeyAndValue(serializeKey(key), hint, keys, values,
                                                getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKeyAndValue(String key, ContainerType type,
                                                                        List<Payload> keys, List<Variant> values) {
        return removeFromContainerByKeyAndValue(serializeKey(key), null, keys, values,
                                                getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key, KeyHintData hint, ContainerType type,
                                                                        List<Payload> keys, List<Variant> values) {
        return removeFromContainerByKeyAndValue(key, hint, keys, values,
                                                getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key, ContainerType type,
                                                                        List<Payload> keys, List<Variant> values) {
        return removeFromContainerByKeyAndValue(key, null, keys, values,
                                                getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Deletes matching keys from the given container.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param containerKey
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return count of successfully removed elements.
     */

    CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                        KeyHintData hint, Payload containerKey,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<Integer> removeFromContainerByKey(String key, KeyHintData hint,
                                                                   Payload keys,
                                                                   int clientId, Duration timeout) {
        return removeFromContainerByKey(serializeKey(key), hint, keys,  clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainerByKey(String key, KeyHintData hint,
                                                                   Payload keys) {
        return removeFromContainerByKey(serializeKey(key), hint, keys,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(String key,
                                                                   Payload keys) {
        return removeFromContainerByKey(serializeKey(key), null, keys,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(byte[] key, KeyHintData hint,
                                                                   Payload keys) {
        return removeFromContainerByKey(key, hint, keys,
                                           getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                                   Payload keys) {
        return removeFromContainerByKey(key, null, keys,
                                           getDefaultClientId(), getDefaultTimeout());
    }


    /**
     * Deletes matching keys from the given container.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param containerKey
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return count of successfully removed elements.
     */

    CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                        KeyHintData hint, Variant containerKey,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<Integer> removeFromContainerByKey(String key, KeyHintData hint,
                                                                Variant keys,
                                                                int clientId, Duration timeout) {
        return removeFromContainerByKey(serializeKey(key), hint, keys,  clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainerByKey(String key, KeyHintData hint,
                                                                Variant keys) {
        return removeFromContainerByKey(serializeKey(key), hint, keys,
                                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(String key,
                                                                Variant keys) {
        return removeFromContainerByKey(serializeKey(key), null, keys,
                                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(byte[] key, KeyHintData hint,
                                                                Variant keys) {
        return removeFromContainerByKey(key, hint, keys,
                                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                                Variant keys) {
        return removeFromContainerByKey(key, null, keys,
                                        getDefaultClientId(), getDefaultTimeout());
    }
}
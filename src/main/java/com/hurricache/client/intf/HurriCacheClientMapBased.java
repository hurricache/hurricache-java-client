package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.Collections;
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
     * @return a {@link CompletableFuture} containing the value bytes.
     */
    CompletableFuture<byte[]> getContainerValue(byte[] key,
                                                KeyHintData hint,
                                                byte[] elementKey,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<byte[]> getContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                        Duration ttl, int clientId, Duration timeout) {
        return getContainerValue(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<byte[]> getContainerValue(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getContainerValue(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getContainerValue(String key, KeyHintData hint, byte[] elementKey) {
        return getContainerValue(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getContainerValue(key, hint, elementKey, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getContainerValue(String key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getContainerValue(serializeKey(key), hint, elementKey, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves and removes a value associated with an element key from a container.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing the deleted value bytes.
     */
    CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key,
                                                         KeyHintData hint,
                                                         byte[] elementKey,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                                 int clientId, Duration timeout) {
        return getAndRemoveContainerValue(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getAndRemoveContainerValue(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveContainerValue(String key, KeyHintData hint, byte[] elementKey) {
        return getAndRemoveContainerValue(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
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
    CompletableFuture<Boolean> containsContainerKey(byte[] key,
                                                    KeyHintData hint,
                                                    byte[] elementKey,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Boolean> containsContainerKey(String key, KeyHintData hint, byte[] elementKey,
                                                            int clientId, Duration timeout) {
        return containsContainerKey(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Boolean> containsContainerKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return containsContainerKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> containsContainerKey(String key, KeyHintData hint, byte[] elementKey) {
        return containsContainerKey(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Updates the value associated with an element key in a container.
     *
     * @param key         target container key in byte array form.
     * @param hint        optional key routing hint.
     * @param elementKey  element key in byte array form.
     * @param value       new value bytes.
     * @param clientId    identifier of the issuing client.
     * @param timeout     execution timeout duration.
     * @return a {@link CompletableFuture} containing the previous value bytes.
     */
    CompletableFuture<byte[]> updateContainerValue(byte[] key,
                                                   KeyHintData hint,
                                                   byte[] elementKey,
                                                   byte[] value,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<byte[]> updateContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                           byte[] value, int clientId, Duration timeout) {
        return updateContainerValue(serializeKey(key), hint, elementKey, value, clientId, timeout);
    }

    default CompletableFuture<byte[]> updateContainerValue(byte[] key, KeyHintData hint, byte[] elementKey,
                                                           byte[] value) {
        return updateContainerValue(key, hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                           byte[] value) {
        return updateContainerValue(serializeKey(key), hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
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
    CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                   KeyHintData hint,
                                                   byte[] elementKey,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, byte[] elementKey,
                                                           int clientId, Duration timeout) {
        return removeFromContainer(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, byte[] elementKey) {
        return removeFromContainer(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, byte[] elementKey) {
        return removeFromContainer(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements to a HashMap container.
     *
     * @param key             target container key in byte array form.
     * @param hint            optional key routing hint.
     * @param container_keys  list of payload keys.
     * @param container_values list of payload values.
     * @param clientId        identifier of the issuing client.
     * @param timeout         execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementHashMap(byte[] key,
                                                 KeyHintData hint,
                                                 List<Payload> container_keys,
                                                 List<Payload> container_values,
                                                 int clientId,
                                                 Duration timeout);

    default CompletableFuture<Integer> addElementHashMap(String key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Payload> container_values,
                                                         int clientId, Duration timeout) {
        return addElementHashMap(serializeKey(key), hint, container_keys, container_values, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementHashMap(byte[] key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Payload> container_values) {
        return addElementHashMap(key, hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(String key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Payload> container_values) {
        return addElementHashMap(serializeKey(key), hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(byte[] key, List<Payload> container_keys,
                                                         List<Payload> container_values) {
        return addElementHashMap(key, null, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(String key, List<Payload> container_keys,
                                                         List<Payload> container_values) {
        return addElementHashMap(serializeKey(key), null, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Deletes matching values/keys from the given container type.
     *
     * @param key          target container key in byte array form.
     * @param hint         optional key routing hint.
     * @param type         container type to operate on.
     * @param values       list of values to match.
     * @param keys         list of keys to match.
     * @param clientId     identifier of the issuing client.
     * @param timeout      execution timeout duration.
     * @return count of successfully removed elements.
     */
    CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                   KeyHintData hint,
                                                   ContainerType type,
                                                   List<Payload> keys,
                                                   List<Payload> values,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys,List<Payload> values,
                                                           int clientId, Duration timeout) {
        return removeFromContainer(serializeKey(key), hint, type, keys,values, clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys, List<Payload> values) {
        return removeFromContainer(serializeKey(key), hint, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(String key, ContainerType type,
                                                           List<Payload> keys, List<Payload> values) {
        return removeFromContainer(serializeKey(key), null, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys, List<Payload> values) {
        return removeFromContainer(key, hint, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key, ContainerType type,
                                                           List<Payload> keys, List<Payload> values) {
        return removeFromContainer(key, null, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }
}
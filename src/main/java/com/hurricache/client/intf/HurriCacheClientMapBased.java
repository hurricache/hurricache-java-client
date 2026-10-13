package com.hurricache.client.intf;

import com.hurricache.client.intf.Variant;
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
     * @return a {@link CompletableFuture} containing the value as Variant.
     */
    CompletableFuture<Variant> getValueInContainerByKey(byte[] key,
                                                        KeyHintData hint,
                                                        byte[] elementKey,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<Variant> getValueInContainerByKey(String key, KeyHintData hint, byte[] elementKey,
                                                                Duration ttl, int clientId, Duration timeout) {
        return getValueInContainerByKey(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Variant> getValueInContainerByKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getValueInContainerByKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValueInContainerByKey(String key, KeyHintData hint, byte[] elementKey) {
        return getValueInContainerByKey(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValueInContainerByKey(byte[] key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getValueInContainerByKey(key, hint, elementKey, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getValueInContainerByKey(String key, KeyHintData hint, byte[] elementKey, int clientId) {
        return getValueInContainerByKey(serializeKey(key), hint, elementKey, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> getAndDeleteValueInContainer(byte[] key,
                                                            KeyHintData hint,
                                                            byte[] elementKey,
                                                            int clientId,
                                                            Duration timeout);

    default CompletableFuture<Variant> getAndDeleteValueInContainer(String key, KeyHintData hint, byte[] elementKey,
                                                                    int clientId, Duration timeout) {
        return getAndDeleteValueInContainer(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Variant> getAndDeleteValueInContainer(byte[] key, KeyHintData hint, byte[] elementKey) {
        return getAndDeleteValueInContainer(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteValueInContainer(String key, KeyHintData hint, byte[] elementKey) {
        return getAndDeleteValueInContainer(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
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
    CompletableFuture<Boolean> existInContainerByKey(byte[] key,
                                                     KeyHintData hint,
                                                     byte[] elementKey,
                                                     int clientId,
                                                     Duration timeout);

    default CompletableFuture<Boolean> existInContainerByKey(String key, KeyHintData hint, byte[] elementKey,
                                                             int clientId, Duration timeout) {
        return existInContainerByKey(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Boolean> existInContainerByKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return existInContainerByKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existInContainerByKey(String key, KeyHintData hint, byte[] elementKey) {
        return existInContainerByKey(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
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
    CompletableFuture<Variant> updateContainerValue(byte[] key,
                                                    KeyHintData hint,
                                                    byte[] elementKey,
                                                    Variant value,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Variant> updateContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                            Variant value, int clientId, Duration timeout) {
        return updateContainerValue(serializeKey(key), hint, elementKey, value, clientId, timeout);
    }

    default CompletableFuture<Variant> updateContainerValue(byte[] key, KeyHintData hint, byte[] elementKey,
                                                            Variant value) {
        return updateContainerValue(key, hint, elementKey, value, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> updateContainerValue(String key, KeyHintData hint, byte[] elementKey,
                                                            Variant value) {
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
    CompletableFuture<Integer> removeInContainerByKey(byte[] key,
                                                      KeyHintData hint,
                                                      byte[] elementKey,
                                                      int clientId,
                                                      Duration timeout);

    default CompletableFuture<Integer> removeInContainerByKey(String key, KeyHintData hint, byte[] elementKey,
                                                              int clientId, Duration timeout) {
        return removeInContainerByKey(serializeKey(key), hint, elementKey, clientId, timeout);
    }

    default CompletableFuture<Integer> removeInContainerByKey(byte[] key, KeyHintData hint, byte[] elementKey) {
        return removeInContainerByKey(key, hint, elementKey, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeInContainerByKey(String key, KeyHintData hint, byte[] elementKey) {
        return removeInContainerByKey(serializeKey(key), hint, elementKey, getDefaultClientId(), getDefaultTimeout());
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
    CompletableFuture<Integer> addElementHashMap(byte[] key,
                                                 KeyHintData hint,
                                                 List<Payload> container_keys,
                                                 List<Variant> container_values,
                                                 int clientId,
                                                 Duration timeout);

    default CompletableFuture<Integer> addElementHashMap(String key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Variant> container_values,
                                                         int clientId, Duration timeout) {
        return addElementHashMap(serializeKey(key), hint, container_keys, container_values, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementHashMap(byte[] key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Variant> container_values) {
        return addElementHashMap(key, hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(String key, KeyHintData hint,
                                                         List<Payload> container_keys,
                                                         List<Variant> container_values) {
        return addElementHashMap(serializeKey(key), hint, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(byte[] key, List<Payload> container_keys,
                                                         List<Variant> container_values) {
        return addElementHashMap(key, null, container_keys, container_values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementHashMap(String key, List<Payload> container_keys,
                                                         List<Variant> container_values) {
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
                                                   List<Variant> values,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys,List<Variant> values,
                                                           int clientId, Duration timeout) {
        return removeFromContainer(serializeKey(key), hint, type, keys,values, clientId, timeout);
    }

    default CompletableFuture<Integer> removeFromContainer(String key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys, List<Variant> values) {
        return removeFromContainer(serializeKey(key), hint, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(String key, ContainerType type,
                                                           List<Payload> keys, List<Variant> values) {
        return removeFromContainer(serializeKey(key), null, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, ContainerType type,
                                                           List<Payload> keys, List<Variant> values) {
        return removeFromContainer(key, hint, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key, ContainerType type,
                                                           List<Payload> keys, List<Variant> values) {
        return removeFromContainer(key, null, type, keys, values,
                                   getDefaultClientId(), getDefaultTimeout());
    }
}
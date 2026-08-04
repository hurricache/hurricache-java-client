package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientMapBased extends HurriCacheClientInterfaceCommon{
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

    default CompletableFuture<Integer> removeFromContainer(String key,KeyHintData hint, ContainerType type,List<Payload> keys, List<Payload> values) {
        return removeFromContainer(serializeKey(key),
                                   hint,
                                   type,
                                   keys,
                                   values,
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }
    default CompletableFuture<Integer> removeFromContainer(String key, ContainerType type,List<Payload> keys, List<Payload> values) {
        return removeFromContainer(serializeKey(key),
                                   null,
                                   type,
                                   keys,
                                   values,
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                           KeyHintData hint,
                                                           ContainerType type,
                                                           List<Payload> keys,
                                                           List<Payload> values) {
        return removeFromContainer(key,
                                   hint,
                                   type,
                                   values,
                                   keys,
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }
    default CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                           ContainerType type,
                                                           List<Payload> keys,
                                                           List<Payload> values) {
        return removeFromContainer(key,
                                   null,
                                   type,
                                   values,
                                   keys,
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }
}
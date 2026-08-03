package com.hurricache.client.intf;

import java.time.Duration;
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


}
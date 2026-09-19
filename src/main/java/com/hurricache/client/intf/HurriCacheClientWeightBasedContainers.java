package com.hurricache.client.intf;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientWeightBasedContainers extends HurriCacheClientInterfaceCommon{
    /**
     * Retrieves an element located at a specific index/position.
     */
    CompletableFuture<Payload> getElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Payload> getElementWithWeight(String key, int pos) {
        return getElementWithWeight(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementWithWeight(String key, KeyHintData hint, int pos) {
        return getElementWithWeight(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementWithWeight(String key, int pos, int clientId) {
        return getElementWithWeight(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementWithWeight(byte[] key, KeyHintData hint, int pos) {
        return getElementWithWeight(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Atomically fetches and removes an element situated at a specific index/position.
     */
    CompletableFuture<Payload> getAndRemoveElementWithWeight(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    default CompletableFuture<Payload> getAndRemoveElementWithWeight(String key, KeyHintData hint, int pos) {
        return getAndRemoveElementWithWeight(key, hint, pos, getDefaultClientId());
    }

    default CompletableFuture<Payload> getAndRemoveElementWithWeight(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndRemoveElementWithWeight(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementWithWeight(byte[] key, KeyHintData hint, int pos) {
        return getAndRemoveElementWithWeight(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link OrderedPayload} elements with weights.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedPayload> data,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> data,
                                                            int clientId, Duration timeout) {
        return addElementWithWeight(serializeKey(key), hint, data,  clientId, timeout);
    }

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), hint, data,  getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> data) {
        return addElementWithWeight(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> data, int clientId) {
        return addElementWithWeight(serializeKey(key), hint, data,  clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> data, int clientId) {
        return addElementWithWeight(key, hint, data, clientId, getDefaultTimeout());
    }
}
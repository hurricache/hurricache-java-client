package com.hurricache.client.intf;

import com.hurricache.grpc.KeyHint;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientDequeBased extends HurriCacheClientInterfaceCommon{
    /**
     * Atomically retrieves and removes the front element of a container (Pop Front / Dequeue).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed payload.
     */
    CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveFront(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getAndRemoveFront(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key) {
        return getAndRemoveFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(byte[] key) {
        return getAndRemoveFront(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, KeyHintData hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, KeyHintData hint, int clientId) {
        return getAndRemoveFront(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId) {
        return getAndRemoveFront(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Gets the element at the head of a sequence.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the head payload.
     */
    CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Payload> getHead(String key) {
        return getHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key) {
        return getHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(String key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key, int clientId) {
        return getHead(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(String key, KeyHintData hint, int clientId) {
        return getHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId) {
        return getHead(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Gets the element at the tail of a sequence.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the tail payload.
     */
    CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Payload> getTail(String key) {
        return getTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key) {
        return getTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(String key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key, int clientId) {
        return getTail(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(String key, KeyHintData hint, int clientId) {
        return getTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId) {
        return getTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Atomically retrieves and removes the tail element of a container (Pop Back).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed payload.
     */
    CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getAndRemoveTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key) {
        return getAndRemoveTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(byte[] key) {
        return getAndRemoveTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, KeyHintData hint) {
        return getAndRemoveTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint) {
        return getAndRemoveTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, KeyHintData hint, int clientId) {
        return getAndRemoveTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId) {
        return getAndRemoveTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Appends a collection of elements to the tail of a container (Push Back).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of payloads to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementToTail(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Payload> data,
                                                        int clientId, Duration timeout) {
        return addElementToTail(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToTail(String key, List<Payload> data) {
        return addElementToTail(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, List<Payload> data) {
        return addElementToTail(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Payload> data, int clientId) {
        return addElementToTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data, int clientId) {
        return addElementToTail(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Prepends a collection of elements to the head of a container (Push Front).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of payloads to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementToHead(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Payload> data,
                                                        int clientId, Duration timeout) {
        return addElementToHead(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToHead(String key, List<Payload> data) {
        return addElementToHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, List<Payload> data) {
        return addElementToHead(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Payload> data) {
        return addElementToHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementToHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Payload> data, int clientId) {
        return addElementToHead(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, KeyHintData hint, List<Payload> data, int clientId) {
        return addElementToHead(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Removes the tail element of a sequence without returning the deleted element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing true if successful.
     */
    CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return removeTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key) {
        return removeTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHintData hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, int clientId) {
        return removeTail(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHintData hint, int clientId) {
        return removeTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId) {
        return removeTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Removes the head element of a sequence without returning the deleted element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing true if successful.
     */
    CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return removeHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key) {
        return removeHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, KeyHintData hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, int clientId) {
        return removeHead(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, KeyHintData hint, int clientId) {
        return removeHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId) {
        return removeHead(key, hint, clientId, getDefaultTimeout());
    }
}
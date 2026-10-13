package com.hurricache.client.intf;

import com.hurricache.grpc.KeyHint;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.hurricache.client.intf.Variant;

public interface HurriCacheClientDequeBased extends HurriCacheClientInterfaceCommon{
    /**
     * Atomically retrieves and removes the front element of a container (Pop Front / Dequeue).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    CompletableFuture<Variant> getAndDeleteHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getAndDeleteHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getAndDeleteHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> getAndDeleteHead(String key) {
        return getAndDeleteHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteHead(byte[] key) {
        return getAndDeleteHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteHead(String key, KeyHintData hint) {
        return getAndDeleteHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteHead(byte[] key, KeyHintData hint) {
        return getAndDeleteHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteHead(String key, KeyHintData hint, int clientId) {
        return getAndDeleteHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteHead(byte[] key, KeyHintData hint, int clientId) {
        return getAndDeleteHead(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Gets the element at the head of a sequence.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the head variant.
     */
    CompletableFuture<Variant> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> getHead(String key) {
        return getHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(byte[] key) {
        return getHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(String key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(byte[] key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(byte[] key, int clientId) {
        return getHead(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(String key, KeyHintData hint, int clientId) {
        return getHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getHead(byte[] key, KeyHintData hint, int clientId) {
        return getHead(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Gets the element at the tail of a sequence.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the tail variant.
     */
    CompletableFuture<Variant> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> getTail(String key) {
        return getTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(byte[] key) {
        return getTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(String key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(byte[] key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(byte[] key, int clientId) {
        return getTail(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(String key, KeyHintData hint, int clientId) {
        return getTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getTail(byte[] key, KeyHintData hint, int clientId) {
        return getTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Atomically retrieves and removes the tail element of a container (Pop Back).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    CompletableFuture<Variant> getAndDeleteTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> getAndDeleteTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return getAndDeleteTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> getAndDeleteTail(String key) {
        return getAndDeleteTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteTail(byte[] key) {
        return getAndDeleteTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteTail(String key, KeyHintData hint) {
        return getAndDeleteTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteTail(byte[] key, KeyHintData hint) {
        return getAndDeleteTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteTail(String key, KeyHintData hint, int clientId) {
        return getAndDeleteTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteTail(byte[] key, KeyHintData hint, int clientId) {
        return getAndDeleteTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Appends a collection of elements to the tail of a container (Push Back).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of Variant values to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementToTail(byte[] key,
                                                KeyHintData hint,
                                                List<Variant> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Variant> data,
                                                        int clientId, Duration timeout) {
        return addElementToTail(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToTail(String key, List<Variant> data) {
        return addElementToTail(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, List<Variant> data) {
        return addElementToTail(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Variant> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Variant> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(String key, KeyHintData hint, List<Variant> data, int clientId) {
        return addElementToTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        return addElementToTail(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Prepends a collection of elements to the head of a container (Push Front).
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of Variant values to add.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementToHead(byte[] key,
                                                KeyHintData hint,
                                                List<Variant> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Variant> data,
                                                        int clientId, Duration timeout) {
        return addElementToHead(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToHead(String key, List<Variant> data) {
        return addElementToHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, List<Variant> data) {
        return addElementToHead(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Variant> data) {
        return addElementToHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, KeyHintData hint, List<Variant> data) {
        return addElementToHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(String key, KeyHintData hint, List<Variant> data, int clientId) {
        return addElementToHead(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToHead(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        return addElementToHead(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Removes the tail element of a sequence without returning the deleted element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the count of deleted elements.
     */
    CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Integer> deleteTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return deleteTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Integer> deleteTail(String key) {
        return deleteTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(byte[] key) {
        return deleteTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(String key, KeyHintData hint) {
        return deleteTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint) {
        return deleteTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(String key, int clientId) {
        return deleteTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(byte[] key, int clientId) {
        return deleteTail(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(String key, KeyHintData hint, int clientId) {
        return deleteTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint, int clientId) {
        return deleteTail(key, hint, clientId, getDefaultTimeout());
    }

    /**
     * Removes the head element of a sequence without returning the deleted element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the count of deleted elements.
     */
    CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Integer> deleteHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return deleteHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Integer> deleteHead(String key) {
        return deleteHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(byte[] key) {
        return deleteHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(String key, KeyHintData hint) {
        return deleteHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint) {
        return deleteHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(String key, int clientId) {
        return deleteHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(byte[] key, int clientId) {
        return deleteHead(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(String key, KeyHintData hint, int clientId) {
        return deleteHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint, int clientId) {
        return deleteHead(key, hint, clientId, getDefaultTimeout());
    }
}
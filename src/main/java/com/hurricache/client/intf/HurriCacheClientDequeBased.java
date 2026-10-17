package com.hurricache.client.intf;

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
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> popHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return popHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> popHead(String key) {
        return popHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popHead(byte[] key) {
        return popHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popHead(String key, KeyHintData hint) {
        return popHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint) {
        return popHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popHead(String key, KeyHintData hint, int clientId) {
        return popHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint, int clientId) {
        return popHead(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> peekHead(String key, KeyHintData hint, int clientId, Duration timeout) {
        return peekHead(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> peekHead(String key) {
        return peekHead(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(byte[] key) {
        return peekHead(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(String key, KeyHintData hint) {
        return peekHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint) {
        return peekHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(String key, int clientId) {
        return peekHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(byte[] key, int clientId) {
        return peekHead(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(String key, KeyHintData hint, int clientId) {
        return peekHead(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint, int clientId) {
        return peekHead(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> peekTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return peekTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> peekTail(String key) {
        return peekTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(byte[] key) {
        return peekTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(String key, KeyHintData hint) {
        return peekTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint) {
        return peekTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(String key, int clientId) {
        return peekTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(byte[] key, int clientId) {
        return peekTail(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(String key, KeyHintData hint, int clientId) {
        return peekTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint, int clientId) {
        return peekTail(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Variant> popTail(String key, KeyHintData hint, int clientId, Duration timeout) {
        return popTail(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Variant> popTail(String key) {
        return popTail(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popTail(byte[] key) {
        return popTail(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popTail(String key, KeyHintData hint) {
        return popTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint) {
        return popTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popTail(String key, KeyHintData hint, int clientId) {
        return popTail(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint, int clientId) {
        return popTail(key, hint, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> pushTail(byte[] key,
                                        KeyHintData hint,
                                        List<Variant> data,
                                        int clientId,
                                        Duration timeout);

    default CompletableFuture<Integer> pushTail(String key, KeyHintData hint, List<Variant> data,
                                                int clientId, Duration timeout) {
        return pushTail(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> pushTail(String key, List<Variant> data) {
        return pushTail(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, List<Variant> data) {
        return pushTail(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(String key, KeyHintData hint, List<Variant> data) {
        return pushTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, KeyHintData hint, List<Variant> data) {
        return pushTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(String key, KeyHintData hint, List<Variant> data, int clientId) {
        return pushTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        return pushTail(key, hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(String key, List<Variant> data, int clientId) {
        return pushTail(serializeKey(key), null, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, List<Variant> data, int clientId) {
        return pushTail(key, null, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(String key, int clientId, List<Variant> data) {
        return pushTail(serializeKey(key), null, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, int clientId, List<Variant> data) {
        return pushTail(key, null, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(String key, KeyHintData hint, int clientId, List<Variant> data) {
        return pushTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushTail(byte[] key, KeyHintData hint, int clientId, List<Variant> data) {
        return pushTail(key, hint, data, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> pushHead(byte[] key,
                                        KeyHintData hint,
                                        List<Variant> data,
                                        int clientId,
                                        Duration timeout);

    default CompletableFuture<Integer> pushHead(String key, KeyHintData hint, List<Variant> data,
                                                int clientId, Duration timeout) {
        return pushHead(serializeKey(key), hint, data, clientId, timeout);
    }

    default CompletableFuture<Integer> pushHead(String key, List<Variant> data) {
        return pushHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushHead(byte[] key, List<Variant> data) {
        return pushHead(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushHead(String key, KeyHintData hint, List<Variant> data) {
        return pushHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushHead(byte[] key, KeyHintData hint, List<Variant> data) {
        return pushHead(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushHead(String key, KeyHintData hint, List<Variant> data, int clientId) {
        return pushHead(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> pushHead(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        return pushHead(key, hint, data, clientId, getDefaultTimeout());
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